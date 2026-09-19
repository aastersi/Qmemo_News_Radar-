"""GDELT Global Quotation Graph collector: cursor, gaps, errors, limits and idempotency."""

import gzip
import json
import sqlite3
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import httpx
import pytest

from qmemo_radar.application.collection import MultiSourceCollector
from qmemo_radar.application.filtering import FilterPolicy
from qmemo_radar.application.pipeline import PipelineThresholds, RadarPipeline
from qmemo_radar.application.ports import SourceCollector
from qmemo_radar.application.runner import StatusReport
from qmemo_radar.domain import PipelineCounters
from qmemo_radar.infrastructure.collectors import gdelt_gqg
from qmemo_radar.infrastructure.collectors.gdelt_gqg import (
    SOURCE_KEY,
    GdeltCursor,
    GdeltQuotationCollector,
)
from qmemo_radar.infrastructure.storage import SQLiteEventRepository
from qmemo_radar.interfaces.cli import execute
from qmemo_radar.interfaces.telegram.render import status_text

NOW = datetime(2026, 9, 16, 12, 30, 30, tzinfo=UTC)
UTC_ZONE = ZoneInfo("UTC")
# Counted by the selection stage after ingestion; the tests here are about the collector.
SELECTION = {"clusters_created", "preselected", "near_duplicates", "same_event"}
LAG = timedelta(minutes=10)  # newest minute ever requested: 12:20


def minute(hhmm: str) -> str:
    return f"20260916{hhmm.replace(':', '')}00"


def article(url: str, *quotes: str, lang: str = "ENGLISH") -> dict[str, object]:
    return {
        "date": "2026-09-16T12:05:59Z",
        "url": url,
        "title": "Budget",
        "lang": lang,
        "quotes": [
            {"pre": "The minister said ", "quote": text, "post": " on Monday."} for text in quotes
        ],
    }


def gz(*rows: object) -> bytes:
    lines = [row if isinstance(row, bytes) else json.dumps(row).encode() for row in rows]
    return gzip.compress(b"\n".join(lines) + b"\n")


FIRST = "We will not raise taxes this year, whatever happens in parliament"
SECOND = "The budget is balanced for the first time in a decade and it stays so"
SPANISH = "Una frase bastante larga para el radar"
FRENCH = "Une phrase assez longue pour le radar"


class Gdelt:
    """Fake data.gdeltproject.org: files by minute, anything else is 404."""

    def __init__(self, files: Mapping[str, bytes | int | Callable[[], httpx.Response]]) -> None:
        self.files = dict(files)
        self.requested: list[str] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        assert request.url.host == "data.gdeltproject.org"
        name = request.url.path.rsplit("/", 1)[-1].removesuffix(".gqg.json.gz")
        self.requested.append(name)
        found = self.files.get(name, 404)
        if callable(found):
            return found()
        if isinstance(found, int):
            return httpx.Response(found)
        return httpx.Response(200, content=found)

    def collector(self, **overrides: object) -> GdeltQuotationCollector:
        async def no_sleep(_: float) -> None:
            return None

        options: dict[str, object] = {
            "safety_lag": LAG,
            "max_minutes_per_run": 60,
            "first_run_lookback": timedelta(minutes=30),  # first minute: 12:00
            "languages": frozenset({"english"}),
            "allow_unknown_language": False,
            "clock": lambda: NOW,
            "sleep": no_sleep,
        } | overrides
        http = httpx.AsyncClient(transport=httpx.MockTransport(self.handler))
        return GdeltQuotationCollector(http, **options)  # type: ignore[arg-type]


def pipeline(collector: SourceCollector, repository: SQLiteEventRepository) -> RadarPipeline:
    return RadarPipeline(
        collector=MultiSourceCollector({"gdelt_gqg": collector}),
        ranker=None,
        repository=repository,
        filter_policy=FilterPolicy(max_age=timedelta(days=36500)),
        thresholds=PipelineThresholds(),
    )


async def cursor_of(repository: SQLiteEventRepository) -> tuple[str | None, int, object]:
    state = GdeltCursor.parse((await repository.get_checkpoints()).get(SOURCE_KEY))
    nxt = state.next.strftime("%Y%m%d%H%M%S") if state.next else None
    return (nxt, state.attempts, state.blocked)


def stored(repository: SQLiteEventRepository) -> list[tuple[object, ...]]:
    with sqlite3.connect(repository._db_path) as db:
        return db.execute(
            "SELECT url, original_text, author_display_name, language, published_at,"
            " raw_payload_json FROM radar_events ORDER BY rowid"
        ).fetchall()


async def test_quotes_of_a_file_are_stored_one_row_each_and_a_rerun_adds_nothing(
    repository: SQLiteEventRepository,
) -> None:
    gdelt = Gdelt(
        {
            minute("12:05"): gz(
                article("https://news.example/a", FIRST, SECOND),
                article("https://news.example/b", FIRST),  # same quote in another article
            )
        }
    )
    collector = gdelt.collector()

    first = await pipeline(collector, repository).run_once(run_id="r1")
    rows = stored(repository)
    second = await pipeline(collector, repository).run_once(run_id="r2")

    assert gdelt.requested[:3] == [minute("12:00"), minute("12:01"), minute("12:02")]
    assert gdelt.requested[20] == minute("12:20") and len(gdelt.requested) == 21
    assert (first.collected, first.inserted, first.duplicates, first.source_errors) == (3, 2, 1, 0)
    assert [row[:4] for row in rows] == [
        ("https://news.example/a", FIRST, None, "ENGLISH"),
        ("https://news.example/a", SECOND, None, "ENGLISH"),
    ]
    # The same quote in article b is provenance of the first row, not a second copy of it.
    with sqlite3.connect(repository._db_path) as db:
        mentions = db.execute(
            "SELECT e.original_text, m.url, m.domain FROM content_mentions m "
            "JOIN radar_events e ON e.id = m.event_id ORDER BY e.rowid, m.url"
        ).fetchall()
    assert mentions == [(FIRST, "https://news.example/b", "news.example")]
    assert rows[0][4] == "2026-09-16T12:05:59+00:00"
    # Only what the columns lack: the quote, URL, language and date are columns already.
    assert json.loads(str(rows[0][5])) == {
        "title": "Budget",
        "pre": "The minister said ",
        "post": " on Monday.",
        "gqg_file": minute("12:05"),
    }
    # The cursor is the next minute to check; the rerun starts there and finds nothing new.
    assert await repository.get_checkpoints() == {SOURCE_KEY: minute("12:21")}
    assert (second.collected, second.inserted) == (0, 0)
    assert len(gdelt.requested) == 21  # 12:21 is still inside the safety window

    metrics = await repository.metrics_since(datetime(2000, 1, 1, tzinfo=UTC))
    assert {k: v for k, v in metrics[SOURCE_KEY].items() if k not in SELECTION} == {
        "articles_seen": 2,
        "collected": 3,
        "exact_duplicates": 1,
        "expected_gaps": 20,
        "files_checked": 21,
        "files_found": 1,
        "inserted": 2,
        "mentions_aggregated": 1,
        "quotes_accepted": 3,
        "quotes_seen": 3,
    }
    health = {item.source_key: item for item in await repository.source_health()}
    assert health[SOURCE_KEY].last_error is None


async def test_repeating_a_file_or_losing_the_checkpoint_creates_no_duplicates(
    repository: SQLiteEventRepository,
) -> None:
    body = gz(article("https://news.example/a", FIRST, FIRST, SECOND))
    gdelt = Gdelt({minute("12:05"): body, minute("12:06"): body})

    first = await pipeline(gdelt.collector(), repository).run_once()
    with sqlite3.connect(repository._db_path) as db:
        db.execute("DELETE FROM source_checkpoints")  # checkpoint lost: whole window again
    again = await pipeline(gdelt.collector(), repository).run_once()

    assert (first.collected, first.inserted, first.duplicates) == (6, 2, 4)
    assert (again.collected, again.inserted, again.duplicates) == (6, 0, 6)
    assert len(stored(repository)) == 2


async def test_404_minutes_are_gaps_and_the_safety_window_is_never_requested(
    repository: SQLiteEventRepository,
) -> None:
    gdelt = Gdelt({})
    collector = gdelt.collector(clock=lambda: NOW, first_run_lookback=timedelta(minutes=12))

    counters = await pipeline(collector, repository).run_once()

    # 12:18..12:20 are older than the lag; 12:21..12:30 might still be published.
    assert gdelt.requested == [minute("12:18"), minute("12:19"), minute("12:20")]
    assert counters.source_errors == 0
    assert await repository.get_checkpoints() == {SOURCE_KEY: minute("12:21")}

    later = gdelt.collector(clock=lambda: NOW + timedelta(minutes=2))
    await pipeline(later, repository).run_once()
    assert gdelt.requested[3:] == [minute("12:21"), minute("12:22")]
    assert await repository.get_checkpoints() == {SOURCE_KEY: minute("12:23")}


@pytest.mark.parametrize(
    ("failure", "code"),
    [
        (503, "gdelt_server_error"),
        (lambda: (_ for _ in ()).throw(httpx.ReadTimeout("slow")), "gdelt_network_error"),
        (403, "gdelt_http_403"),
    ],
)
async def test_a_failing_minute_is_retried_never_skipped(
    repository: SQLiteEventRepository, failure: object, code: str
) -> None:
    gdelt = Gdelt(
        {
            minute("12:02"): gz(article("https://news.example/a", FIRST)),
            minute("12:04"): failure,  # type: ignore[dict-item]
            minute("12:06"): gz(article("https://news.example/b", SECOND)),
        }
    )

    first = await pipeline(gdelt.collector(), repository).run_once()
    retries = gdelt.requested.count(minute("12:04"))
    assert (first.inserted, first.source_errors) == (1, 1)
    # Progress up to the failure is kept, the failing minute becomes the cursor.
    assert await cursor_of(repository) == (minute("12:04"), 1, {})
    assert minute("12:05") not in gdelt.requested

    second = await pipeline(gdelt.collector(), repository).run_once()
    assert gdelt.requested.count(minute("12:04")) == 2 * retries
    assert await cursor_of(repository) == (minute("12:04"), 2, {})
    [health] = [h for h in await repository.source_health() if h.source_key == SOURCE_KEY]
    # No progress in the second run, so the failures keep counting up.
    assert (health.last_error, health.consecutive_failures) == (code, 2)
    assert second.source_errors == 1

    gdelt.files.pop(minute("12:04"))
    third = await pipeline(gdelt.collector(), repository).run_once()
    assert third.inserted == 1 and third.source_errors == 0
    assert await repository.get_checkpoints() == {SOURCE_KEY: minute("12:21")}
    metrics = await repository.metrics_since(datetime(2000, 1, 1, tzinfo=UTC))
    assert metrics[SOURCE_KEY]["source_errors"] == 2
    # Run 1: 12:00, 12:01, 12:03. Run 3: 12:04 (now missing), 12:05 and 12:07..12:20.
    assert metrics[SOURCE_KEY]["expected_gaps"] == 3 + 0 + 16
    assert retries == (3 if code != "gdelt_http_403" else 1)


async def test_malformed_rows_and_filtered_languages_are_counted_and_skipped(
    repository: SQLiteEventRepository,
) -> None:
    quote_too_long = "x" * (gdelt_gqg.MAX_QUOTE_CHARS + 1)
    gdelt = Gdelt(
        {
            minute("12:05"): gz(
                b"{not json",
                b"[1, 2]",
                b"\xff\xfe broken utf-8",
                {"url": "https://news.example/no-quotes", "lang": "ENGLISH", "date": "x"},
                article("https://news.example/es", SPANISH, lang="SPANISH"),
                article("https://news.example/unknown", "A quote without any language", lang=""),
                article("not a url", "A quote from an article without a valid address"),
                article("https://news.example/ok", FIRST, "", quote_too_long),
            )
        }
    )

    counters = await pipeline(gdelt.collector(), repository).run_once()

    assert counters.inserted == 1 and counters.source_errors == 0
    metrics = await repository.metrics_since(datetime(2000, 1, 1, tzinfo=UTC))
    counted = metrics[SOURCE_KEY]
    skip = {"files_checked", "expected_gaps", *SELECTION}
    assert {k: v for k, v in counted.items() if k not in skip} == {
        "articles_language_skipped": 2,
        "articles_seen": 5,
        "collected": 1,
        "files_found": 1,
        "inserted": 1,
        "malformed_rows": 4,
        "quotes_accepted": 1,
        "quotes_rejected": 2,  # empty quote and the invalid URL
        "quotes_seen": 6,
        "quotes_too_long": 1,
    }


async def test_several_languages_and_unknown_language_can_be_allowed(
    repository: SQLiteEventRepository,
) -> None:
    gdelt = Gdelt(
        {
            minute("12:05"): gz(
                article("https://news.example/en", FIRST),
                article("https://news.example/es", SPANISH, lang="Spanish"),
                article("https://news.example/fr", FRENCH, lang="FRENCH"),
                article("https://news.example/none", SECOND, lang=""),
            )
        }
    )
    collector = gdelt.collector(
        languages=frozenset({"english", "spanish"}), allow_unknown_language=True
    )

    await pipeline(collector, repository).run_once()

    assert [row[3] for row in stored(repository)] == ["ENGLISH", "Spanish", None]
    everything = Gdelt(gdelt.files).collector(languages=None, allow_unknown_language=False)
    [found, _] = await everything.collect({})
    assert len(found.items) == 3  # every named language, still no unknown one


async def test_a_corrupt_or_oversized_file_is_counted_and_passed(
    repository: SQLiteEventRepository, monkeypatch: pytest.MonkeyPatch
) -> None:
    good = gz(article("https://news.example/a", FIRST), article("https://news.example/b", SECOND))
    truncated_gzip = good[: len(good) // 2]
    gdelt = Gdelt(
        {
            minute("12:03"): b"this is not gzip at all",
            minute("12:04"): truncated_gzip,
            minute("12:05"): lambda: httpx.Response(200, content=b"x" * 2048),
            minute("12:06"): good,
        }
    )
    monkeypatch.setattr(gdelt_gqg, "MAX_COMPRESSED_BYTES", 1024)

    counters = await pipeline(gdelt.collector(), repository).run_once()

    assert counters.source_errors == 0 and counters.inserted == 2
    assert await repository.get_checkpoints() == {SOURCE_KEY: minute("12:21")}
    metrics = (await repository.metrics_since(datetime(2000, 1, 1, tzinfo=UTC)))[SOURCE_KEY]
    assert (metrics["files_corrupt"], metrics["files_oversized"]) == (2, 1)
    assert metrics["files_found"] == 3


async def test_decompressed_size_and_article_count_are_capped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = [article(f"https://news.example/{n}", f"{FIRST} number {n}") for n in range(10)]
    gdelt = Gdelt({minute("12:05"): gz(*rows)})
    monkeypatch.setattr(gdelt_gqg, "MAX_ARTICLES_PER_FILE", 4)
    [found, final] = await gdelt.collector().collect({})
    assert len(found.items) == 4 and final.stats["files_truncated"] == 1

    monkeypatch.setattr(gdelt_gqg, "MAX_ARTICLES_PER_FILE", 100)
    monkeypatch.setattr(gdelt_gqg, "MAX_DECOMPRESSED_BYTES", 600)
    [found, final] = await gdelt.collector().collect({})
    assert 0 < len(found.items) < 10 and final.stats["files_truncated"] == 1

    monkeypatch.setattr(gdelt_gqg, "MAX_DECOMPRESSED_BYTES", 10**9)
    monkeypatch.setattr(gdelt_gqg, "MAX_LINE_BYTES", 400)
    huge = article("https://news.example/huge", "y" * 1000)
    gdelt.files[minute("12:05")] = gz(rows[0], huge, rows[1])
    [found, final] = await gdelt.collector().collect({})
    assert len(found.items) == 2 and final.stats["malformed_rows"] == 1


async def test_catch_up_after_downtime_is_bounded_per_run(
    repository: SQLiteEventRepository,
) -> None:
    gdelt = Gdelt({minute("10:30"): gz(article("https://news.example/old", FIRST))})
    await repository.record_source_result(SOURCE_KEY, cursor=minute("10:00"), error_code=None)
    collector = gdelt.collector(max_minutes_per_run=45)

    for expected in ("10:45", "11:30", "12:15", "12:21", "12:21"):
        await pipeline(collector, repository).run_once()
        assert await repository.get_checkpoints() == {SOURCE_KEY: minute(expected)}

    assert len(gdelt.requested) == 141 == len(set(gdelt.requested))  # 10:00..12:20, once each
    assert len(stored(repository)) == 1


async def test_an_unreadable_cursor_restarts_from_the_lookback_window() -> None:
    gdelt = Gdelt({})
    fetches = await gdelt.collector().collect({SOURCE_KEY: "garbage"})
    assert gdelt.requested[0] == minute("12:00")
    assert fetches[-1].cursor == minute("12:21")


def test_gdelt_is_opt_in_and_needs_no_key() -> None:
    from qmemo_radar.bootstrap import SourceContext, build_collector, enabled_sources
    from qmemo_radar.config import RadarSettings, SourcesConfig

    off = RadarSettings(_env_file=None)  # type: ignore[call-arg]
    on = RadarSettings(_env_file=None, gdelt_enabled=True, gdelt_languages="English, spanish")  # type: ignore[call-arg]
    assert enabled_sources(off, SourcesConfig()) == []
    assert enabled_sources(on, SourcesConfig()) == ["gdelt_gqg"]
    assert on.gdelt_language_set == frozenset({"english", "spanish"})
    assert RadarSettings(_env_file=None, gdelt_languages="*").gdelt_language_set is None  # type: ignore[call-arg]
    context = SourceContext(on, SourcesConfig(), x_client=None, free_http=httpx.AsyncClient())
    assert build_collector(context).names == ["gdelt_gqg"]


async def test_poison_rows_are_skipped_and_never_stall_the_cursor(
    repository: SQLiteEventRepository,
) -> None:
    # Regression: a lone surrogate or a deeply nested row used to crash the collector or the
    # SQLite insert on every run, so the cursor never moved.
    surrogate_quote = article("https://news.example/s1", FIRST)
    surrogate_title = article("https://news.example/s2", SECOND) | {"title": "\ud800"}
    surrogate_quote["quotes"] = [{"quote": "Lone \ud800 surrogate in the quote"}]
    deep = b'{"quotes": ' + b"[" * 200_000 + b"]" * 200_000 + b"}"
    normal = "A perfectly normal quote that must be stored"
    good = article("https://news.example/good", normal)
    rows = [json.dumps(row).encode() for row in (surrogate_quote, surrogate_title, good)]
    body = gzip.compress(b"\n".join([rows[0], rows[1], deep, rows[2]]))
    gdelt = Gdelt({minute("12:05"): body})

    first = await pipeline(gdelt.collector(), repository).run_once()
    with sqlite3.connect(repository._db_path) as db:
        db.execute("DELETE FROM source_checkpoints")
    second = await pipeline(gdelt.collector(), repository).run_once()

    assert (first.inserted, first.source_errors) == (1, 0)
    assert (second.inserted, second.source_errors) == (0, 0)
    assert [row[1] for row in stored(repository)] == [normal]
    assert await repository.get_checkpoints() == {SOURCE_KEY: minute("12:21")}
    metrics = (await repository.metrics_since(datetime(2000, 1, 1, tzinfo=UTC)))[SOURCE_KEY]
    assert metrics["malformed_rows"] == 2  # the deep row, once per run
    assert metrics["quotes_rejected"] == 2  # the surrogate quote, once per run
    assert metrics["invalid_items"] == 2  # the surrogate title reaches the pipeline and stops there


async def test_a_cursor_or_clock_ahead_of_gdelt_is_an_error_not_a_gap(
    repository: SQLiteEventRepository,
) -> None:
    # Regression: a future cursor idled while /status looked healthy, and a fast local clock
    # recorded not-yet-published minutes as gaps, losing those files for good.
    gdelt = Gdelt({})
    await repository.record_source_result(SOURCE_KEY, cursor="20270101000000", error_code=None)
    ahead = await pipeline(gdelt.collector(), repository).run_once()
    assert (ahead.source_errors, gdelt.requested) == (1, [])
    health = {h.source_key: h for h in await repository.source_health()}
    assert health[SOURCE_KEY].last_error == "gdelt_cursor_ahead"

    def dated(moment: datetime) -> Gdelt:
        header = {"Date": moment.strftime("%a, %d %b %Y %H:%M:%S GMT")}
        minutes = [f"{h}:{m:02d}" for h in (11, 12) for m in range(60)]
        missing = lambda: httpx.Response(404, headers=header)  # noqa: E731
        return Gdelt({minute(hhmm): missing for hhmm in minutes})

    late = dated(NOW - timedelta(minutes=30))  # GDELT's clock: 12:00:30
    await repository.record_source_result(SOURCE_KEY, cursor=minute("11:50"), error_code=None)
    skewed = await pipeline(late.collector(), repository).run_once()
    assert skewed.source_errors == 1
    # By GDELT's clock only 11:50 and 11:51 are past the lag (plus one minute of tolerance).
    assert await repository.get_checkpoints() == {SOURCE_KEY: minute("11:52")}
    health = {h.source_key: h for h in await repository.source_health()}
    assert health[SOURCE_KEY].last_error == "gdelt_clock_ahead"

    synced = dated(NOW)
    await pipeline(synced.collector(), repository).run_once()
    assert await repository.get_checkpoints() == {SOURCE_KEY: minute("12:21")}


async def test_a_cursor_with_seconds_is_floored_to_its_minute() -> None:
    gdelt = Gdelt({})
    await gdelt.collector().collect({SOURCE_KEY: "20260916121530"})
    assert gdelt.requested[0] == minute("12:15")


async def test_skipping_an_oversized_line_stops_at_the_decompression_limit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Regression: the skip loop decompressed a 1 GiB single-line bomb completely first.
    body = gzip.compress(b"z" * (64 * 1024 * 1024))
    reads = 0
    original = gzip.GzipFile.readline

    def counting(self: gzip.GzipFile, size: int = -1) -> bytes:
        nonlocal reads
        reads += 1
        return original(self, size)

    monkeypatch.setattr(gzip.GzipFile, "readline", counting)
    monkeypatch.setattr(gdelt_gqg, "MAX_LINE_BYTES", 64 * 1024)
    monkeypatch.setattr(gdelt_gqg, "MAX_DECOMPRESSED_BYTES", 1024 * 1024)
    [found, final] = await Gdelt({minute("12:05"): body}).collector().collect({})
    assert final.stats["files_truncated"] == 1 and found.items == ()
    assert reads < 40  # ~1 MB of 64 KB reads, not the whole 64 MB


def test_gdelt_must_check_at_least_one_collection_interval_per_run() -> None:
    from qmemo_radar.config import RadarSettings

    with pytest.raises(ValueError, match="RADAR_GDELT_MAX_MINUTES_PER_RUN"):
        RadarSettings(
            _env_file=None,  # type: ignore[call-arg]
            gdelt_enabled=True,
            collect_interval_minutes=90,
        )
    RadarSettings(_env_file=None, collect_interval_minutes=90)  # type: ignore[call-arg]


async def test_a_permanently_failing_minute_becomes_a_visible_blocked_gap(
    repository: SQLiteEventRepository, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Regression: one minute answering 403 forever stopped the cursor forever.
    gdelt = Gdelt(
        {
            minute("12:04"): 403,
            minute("12:06"): gz(article("https://news.example/b", SECOND)),
        }
    )
    for _ in range(2):  # below the threshold: the cursor waits on 12:04
        await pipeline(gdelt.collector(), repository).run_once()
    assert await cursor_of(repository) == (minute("12:04"), 2, {})

    third = await pipeline(gdelt.collector(), repository).run_once()
    nxt, attempts, blocked = await cursor_of(repository)
    assert (nxt, attempts, third.inserted, third.source_errors) == (minute("12:21"), 0, 1, 0)
    assert blocked == {
        minute("12:04"): {"reason": "gdelt_http_403", "attempts": 3, "since": NOW.isoformat()}
    }

    [health] = [h for h in await repository.source_health() if h.source_key == SOURCE_KEY]
    assert (health.blocked_gaps, health.consecutive_failures) == (1, 0)
    report = StatusReport(
        paused=False,
        heartbeat_at=None,
        last_run=None,
        last_success=None,
        sources=[health],
        today=PipelineCounters(),
        sent_today=0,
        outbox_approved=0,
        qmemo_publishing_enabled=False,
        x_publishing_enabled=False,
    )
    assert "gdelt:gqg: работает; заблокировано минут: 1" in status_text(report, timezone=UTC_ZONE)

    # Every later run gives it exactly one more try; the data is never silently dropped.
    before = gdelt.requested.count(minute("12:04"))
    await pipeline(gdelt.collector(), repository).run_once()
    assert gdelt.requested.count(minute("12:04")) == before + 1
    assert (await cursor_of(repository))[2][minute("12:04")]["attempts"] == 4

    gdelt.files[minute("12:04")] = gz(article("https://news.example/late", FIRST))
    recovered = await pipeline(gdelt.collector(), repository).run_once()
    assert recovered.inserted == 1
    assert await cursor_of(repository) == (minute("12:21"), 0, {})
    metrics = await repository.metrics_since(datetime(2000, 1, 1, tzinfo=UTC))
    assert (metrics[SOURCE_KEY]["gaps_blocked"], metrics[SOURCE_KEY]["gaps_recovered"]) == (1, 1)


async def test_an_outage_fills_the_blocked_list_then_stops_visibly(
    repository: SQLiteEventRepository, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(gdelt_gqg, "MAX_BLOCKED_MINUTES", 1)
    gdelt = Gdelt({minute(f"12:{m:02d}"): 503 for m in range(21)})
    for _ in range(3):
        await pipeline(gdelt.collector(), repository).run_once()
    assert await cursor_of(repository) == (minute("12:01"), 1, {minute("12:00"): ANY_ENTRY})

    for _ in range(2):
        last = await pipeline(gdelt.collector(), repository).run_once()
    # 12:01 reached the threshold, but the list is full: the collector stops and says why.
    assert await cursor_of(repository) == (minute("12:01"), 3, {minute("12:00"): ANY_ENTRY})
    health = {item.source_key: item for item in await repository.source_health()}
    assert health[SOURCE_KEY].last_error == "gdelt_blocked_gaps_full"
    assert last.source_errors == 1


async def test_the_gaps_command_lists_and_skips_blocked_minutes_on_request(
    repository: SQLiteEventRepository, capsys: pytest.CaptureFixture[str]
) -> None:
    gdelt = Gdelt({minute("12:04"): 403})
    for _ in range(3):
        await pipeline(gdelt.collector(), repository).run_once()

    assert await execute("gaps", db_path=repository._db_path) == 0
    listed = json.loads(capsys.readouterr().out)
    assert list(listed["blocked"]) == [minute("12:04")] and listed["skipped_now"] == []

    assert await execute("gaps", db_path=repository._db_path, skip=["all"]) == 0
    done = json.loads(capsys.readouterr().out)
    assert (done["blocked"], done["skipped_now"]) == ({}, [minute("12:04")])
    assert done["skipped_before"][minute("12:04")]["reason"] == "gdelt_http_403"
    assert await cursor_of(repository) == (minute("12:21"), 0, {})
    # Skipped means skipped: the next run does not request it again.
    before = gdelt.requested.count(minute("12:04"))
    await pipeline(gdelt.collector(), repository).run_once()
    assert gdelt.requested.count(minute("12:04")) == before


def test_skipping_a_blocked_minute_keeps_a_record() -> None:
    state = GdeltCursor.parse(
        '{"next": "20260916122100", "attempts": 0, "blocked": {"20260916120400":'
        ' {"reason": "gdelt_http_403", "attempts": 7, "since": "x"}}, "skipped": {}}'
    )
    assert state.skip(["20260916120400", "20260916120500"], at=NOW) == ["20260916120400"]
    again = GdeltCursor.parse(state.dump())
    assert again.blocked == {}
    assert again.skipped["20260916120400"]["skipped_at"] == NOW.isoformat()
    assert again.skipped["20260916120400"]["reason"] == "gdelt_http_403"
    # A plain cursor from before blocked gaps existed still reads, and writes back unchanged.
    assert GdeltCursor.parse("20260916122100").dump() == "20260916122100"


class _Any:
    def __eq__(self, other: object) -> bool:
        return True


ANY_ENTRY = _Any()


async def test_one_permanently_blocked_minute_does_not_hold_up_the_others(
    repository: SQLiteEventRepository,
) -> None:
    # Independent review: retries stopped at the first failure, so a minute failing forever
    # kept every later blocked minute from ever being retried.
    blocked = {
        minute("12:00"): {"reason": "gdelt_http_403", "attempts": 3, "since": "x"},
        minute("12:01"): {"reason": "gdelt_server_error", "attempts": 3, "since": "x"},
    }
    await repository.record_source_result(
        SOURCE_KEY,
        cursor=GdeltCursor(next=datetime(2026, 9, 16, 12, 21, tzinfo=UTC), blocked=blocked).dump(),
        error_code=None,
    )
    gdelt = Gdelt(
        {minute("12:00"): 403, minute("12:01"): gz(article("https://news.example/late", FIRST))}
    )

    counters = await pipeline(gdelt.collector(), repository).run_once()

    assert counters.inserted == 1
    assert list((await cursor_of(repository))[2]) == [minute("12:00")]


async def test_an_outage_ends_the_retries_of_blocked_minutes_for_the_run(
    repository: SQLiteEventRepository,
) -> None:
    blocked = {
        minute(f"12:0{n}"): {"reason": "gdelt_server_error", "attempts": 3, "since": "x"}
        for n in range(3)
    }
    await repository.record_source_result(
        SOURCE_KEY,
        cursor=GdeltCursor(next=datetime(2026, 9, 16, 12, 21, tzinfo=UTC), blocked=blocked).dump(),
        error_code=None,
    )
    gdelt = Gdelt({minute(f"12:0{n}"): 503 for n in range(3)})

    await pipeline(gdelt.collector(), repository).run_once()

    assert [gdelt.requested.count(minute(f"12:0{n}")) for n in range(3)] == [1, 0, 0]

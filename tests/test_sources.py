"""Source registry: independent collectors, isolated failures, own checkpoints, no duplicates."""

import sqlite3
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import httpx
import pytest

from qmemo_radar.application.budget import BudgetGuard, PaidFeature
from qmemo_radar.application.collection import MultiSourceCollector
from qmemo_radar.application.filtering import FilterPolicy
from qmemo_radar.application.normalization import build_candidate
from qmemo_radar.application.pipeline import PipelineThresholds, RadarPipeline
from qmemo_radar.application.ports import SourceCollector
from qmemo_radar.application.selection import SelectionPolicy
from qmemo_radar.bootstrap import (
    SOURCE_REGISTRY,
    SourceContext,
    SourceRegistration,
    build_collector,
    enabled_sources,
)
from qmemo_radar.config import RadarSettings, SourcesConfig
from qmemo_radar.domain import CostEntry, RawSourceItem, SourceFetch, SourceType
from qmemo_radar.infrastructure.collectors.x_api import XApiClient
from qmemo_radar.infrastructure.ranking import DeterministicFixtureRanker
from qmemo_radar.infrastructure.storage import SQLiteEventRepository

X_SOURCES = SourcesConfig.model_validate(
    {"x": {"accounts": [{"handle": "founder"}], "queries": [{"name": "q", "query": "q"}]}}
)
X_ON = {"paid_sources_enabled": True, "x_paid_search_enabled": True}


def article(source: SourceType, number: int) -> RawSourceItem:
    return RawSourceItem(
        source=source,
        external_id=f"{source.value}-{number}",
        url=f"https://news.example/{source.value}/{number}",
        author_display_name="Newsroom",
        original_text=f'The minister said: "Budget item {number} of {source.value} is final."',
        published_at=datetime.now(UTC) - timedelta(minutes=5),
    )


class Feed:
    """A free source that, like an RSS feed, returns its whole window on every poll."""

    def __init__(self, key: str, items: list[RawSourceItem]) -> None:
        self.key = key
        self.items = items
        self.seen: list[str | None] = []

    async def collect(self, checkpoints: Mapping[str, str]) -> list[SourceFetch]:
        self.seen.append(checkpoints.get(self.key))
        cursor = str(len(self.items))
        return [SourceFetch(source_key=self.key, items=tuple(self.items), cursor=cursor)]


class Crashing:
    async def collect(self, checkpoints: Mapping[str, str]) -> list[SourceFetch]:
        raise RuntimeError("parser bug")


class Reporting:
    async def collect(self, checkpoints: Mapping[str, str]) -> list[SourceFetch]:
        return [SourceFetch(source_key="bluesky:firehose", error_code="network_error")]


def pipeline(collector: SourceCollector, repository: SQLiteEventRepository) -> RadarPipeline:
    return RadarPipeline(
        collector=collector,
        ranker=DeterministicFixtureRanker(),
        repository=repository,
        filter_policy=FilterPolicy(max_age=timedelta(hours=1)),
        thresholds=PipelineThresholds(),
        # These tests are about sources: every stored story is ranked, none is preselected out.
        selection=SelectionPolicy(min_preselect_score=0),
    )


SELECTION_METRICS = {"clusters_created", "preselected", "near_duplicates", "same_event"}


def _flow(values: Mapping[str, int]) -> dict[str, int]:
    return {name: value for name, value in values.items() if name not in SELECTION_METRICS}


def settings(**values: object) -> RadarSettings:
    return RadarSettings(_env_file=None, **values)  # type: ignore[arg-type]


async def test_collectors_run_together_fail_alone_and_keep_their_own_checkpoints(
    repository: SQLiteEventRepository,
) -> None:
    rss = Feed("rss:wire", [article(SourceType.RSS, n) for n in range(3)])
    gdelt = Feed("gdelt:gqg", [article(SourceType.GDELT, n) for n in range(2)])
    collector = MultiSourceCollector(
        {"rss": rss, "broken": Crashing(), "gdelt": gdelt, "bluesky": Reporting()}
    )

    first = await pipeline(collector, repository).run_once()
    second = await pipeline(collector, repository).run_once()

    assert (first.collected, first.inserted, first.source_errors) == (5, 5, 2)
    assert (second.collected, second.inserted, second.duplicates) == (5, 0, 5)
    assert await repository.get_checkpoints() == {"rss:wire": "3", "gdelt:gqg": "2"}
    assert rss.seen == [None, "3"] and gdelt.seen == [None, "2"]
    health = {item.source_key: item for item in await repository.source_health()}
    assert health["broken"].last_error == "collector_failed:RuntimeError"
    assert health["broken"].consecutive_failures == 2
    assert health["bluesky:firehose"].last_error == "network_error"
    assert health["rss:wire"].consecutive_failures == 0
    assert sum((await repository.count_by_status()).values()) == 5


def test_disabled_x_is_never_built_and_needs_no_credentials() -> None:
    assert enabled_sources(settings(), X_SOURCES) == []
    # Paid flags off: no token, no client, and building the registry still succeeds.
    assert build_collector(SourceContext(settings(), X_SOURCES, x_client=None)).names == []
    # Flags on but nothing configured in sources.yaml: still disabled.
    assert enabled_sources(settings(**X_ON), SourcesConfig()) == []

    assert enabled_sources(settings(**X_ON), X_SOURCES) == ["x_search"]
    with pytest.raises(ValueError, match="RADAR_X_BEARER_TOKEN"):
        build_collector(SourceContext(settings(**X_ON), X_SOURCES, x_client=None))


async def test_free_collector_keeps_working_when_the_budget_is_exhausted(
    repository: SQLiteEventRepository,
) -> None:
    since = datetime(2000, 1, 1, tzinfo=UTC)
    full = CostEntry(
        provider="x",
        operation="earlier",
        units=2000,
        estimated_cost_usd=Decimal(10),
        created_at=datetime.now(UTC),
    )
    assert await repository.reserve_cost(full, since=since, limit_usd=Decimal(10))
    requests: list[httpx.Request] = []
    x_http = httpx.AsyncClient(
        base_url="https://api.x.com",
        transport=httpx.MockTransport(lambda r: requests.append(r) or httpx.Response(200)),
    )
    guard = BudgetGuard(
        repository,
        enabled=frozenset(PaidFeature),
        hard_limit_usd=Decimal(10),
        target_usd=Decimal(0),
    )
    rss = Feed("rss:wire", [article(SourceType.RSS, 1)])
    registry = (
        *SOURCE_REGISTRY,
        SourceRegistration("rss", lambda _settings, _sources: True, lambda _context: rss),
    )
    context = SourceContext(settings(**X_ON), X_SOURCES, XApiClient(x_http, guard=guard))

    counters = await pipeline(build_collector(context, registry), repository).run_once()

    assert counters.inserted == 1 and counters.source_errors == 2
    assert requests == []
    health = {item.source_key: item.last_error for item in await repository.source_health()}
    assert health == {
        "account:founder": "hard_limit_reached",
        "query:q": "hard_limit_reached",
        "rss:wire": None,
        # Each collector that ran without crashing also reports itself healthy.
        "rss": None,
        "x_search": None,
    }


async def test_ingestion_metrics_per_source(
    repository: SQLiteEventRepository, caplog: pytest.LogCaptureFixture
) -> None:
    def rss(number: int, text: str, *, minutes_ago: int = 5) -> RawSourceItem:
        return article(SourceType.RSS, number).model_copy(
            update={
                "original_text": text,
                "published_at": datetime.now(UTC) - timedelta(minutes=minutes_ago),
            }
        )

    shared = 'The minister said: "The wire story is identical everywhere today."'
    feed = Feed(
        "rss:wire",
        [
            rss(1, shared),
            rss(2, 'The minister said: "An old statement from yesterday."', minutes_ago=600),
            rss(3, "Too short"),
            rss(4, shared),  # same text, different article
            rss(5, 'The minister said: "A second fresh statement for the radar."'),
            rss(5, 'The minister said: "A second fresh statement for the radar."'),  # repeated id
        ],
    )
    gdelt = Feed(
        "gdelt:gqg",
        [
            article(SourceType.GDELT, 1).model_copy(update={"original_text": shared}),
            article(SourceType.GDELT, 2),
        ],
    )
    collector = MultiSourceCollector({"rss": feed, "gdelt": gdelt, "broken": Crashing()})

    first = await pipeline(collector, repository).run_once(run_id="run-1")
    caplog.clear()
    with caplog.at_level("INFO"):
        second = await pipeline(collector, repository).run_once(run_id="run-2")

    # "Too short" is rejected by the gate before storage; the two copies of the shared text
    # are mentions of its first row, not rows of their own.
    assert (first.collected, first.inserted, first.duplicates, first.filtered) == (8, 4, 3, 2)
    assert (first.source_errors, first.scored) == (1, 3)
    assert (second.collected, second.inserted, second.duplicates, second.filtered) == (8, 0, 7, 1)
    metrics = await repository.metrics_since(datetime(2000, 1, 1, tzinfo=UTC))
    assert {key: _flow(values) for key, values in metrics.items() if _flow(values)} == {
        "broken": {"source_errors": 2},
        "gdelt:gqg": {
            "collected": 4,
            "exact_duplicates": 3,
            "inserted": 1,
            "mentions_aggregated": 1,
        },
        "rss:wire": {
            "collected": 12,
            "exact_duplicates": 7,
            "filtered": 1,
            "inserted": 3,
            "mentions_aggregated": 1,
            "rejected_too_short": 2,
        },
    }
    with sqlite3.connect(repository._db_path) as db:
        rows = db.execute(
            "SELECT external_id, status, filter_reason, duplicate_of_event_id IS NOT NULL "
            "FROM radar_events ORDER BY rowid"
        ).fetchall()
    assert rows == [
        ("rss-1", "SHORTLISTED", None, 0),
        ("rss-2", "FILTERED_OUT", "too_old", 0),
        ("rss-5", "SHORTLISTED", None, 0),
        ("gdelt-2", "SHORTLISTED", None, 0),
    ]
    with sqlite3.connect(repository._db_path) as db:
        copies = db.execute(
            "SELECT e.external_id, m.source_key, m.url FROM content_mentions m "
            "JOIN radar_events e ON e.id = m.event_id WHERE m.url != e.url ORDER BY m.url"
        ).fetchall()
    assert copies == [
        ("rss-1", "gdelt:gqg", "https://news.example/gdelt/1"),
        ("rss-1", "rss:wire", "https://news.example/rss/4"),
    ]
    logged = {
        getattr(record, "source_key", None): getattr(record, "result", None)
        for record in caplog.records
        if record.getMessage() == "source collected"
    }
    assert logged["rss:wire"] == "collected=6 rejected_too_short=1 exact_duplicates=5"


async def test_collector_recovering_from_a_crash_clears_its_error(
    repository: SQLiteEventRepository,
) -> None:
    class Flaky:
        crash = True

        async def collect(self, checkpoints: Mapping[str, str]) -> list[SourceFetch]:
            if self.crash:
                raise RuntimeError("temporary")
            return []

    flaky = Flaky()
    collector = MultiSourceCollector({"flaky": flaky})
    await pipeline(collector, repository).run_once()
    flaky.crash = False
    await pipeline(collector, repository).run_once()

    [health] = await repository.source_health()
    assert (health.source_key, health.consecutive_failures) == ("flaky", 0)


async def test_copy_of_an_ignored_original_is_stored_without_a_dangling_link(
    repository: SQLiteEventRepository,
) -> None:
    stored = build_candidate(article(SourceType.RSS, 1))
    assert await repository.add_event(stored)  # e.g. a manual link stored between two awaits
    # The ingest batch still believes its own copy of item 1 is the original.
    original = build_candidate(article(SourceType.RSS, 1))
    copy = build_candidate(article(SourceType.GDELT, 2)).model_copy(
        update={"duplicate_of_event_id": original.event_id}
    )

    assert await repository.add_events([original, copy]) == 1
    with sqlite3.connect(repository._db_path) as db:
        rows = db.execute(
            "SELECT id, external_id, duplicate_of_event_id FROM radar_events ORDER BY rowid"
        ).fetchall()
    assert rows == [(stored.event_id, "rss-1", None), (copy.event_id, "gdelt-2", None)]


async def test_quotes_of_one_article_share_its_url_but_other_sources_do_not(
    repository: SQLiteEventRepository,
) -> None:
    def quote(source: SourceType, number: int) -> RawSourceItem:
        return article(source, number).model_copy(
            update={
                "url": "https://news.example/story",
                "original_text": f"{source.value} quote {number} " * 3,
            }
        )

    gdelt = Feed("gdelt:gqg", [quote(SourceType.GDELT, 1), quote(SourceType.GDELT, 2)])
    rss = Feed("rss:wire", [quote(SourceType.RSS, 1), quote(SourceType.RSS, 2)])

    counters = await pipeline(MultiSourceCollector({"g": gdelt, "r": rss}), repository).run_once()

    assert (counters.inserted, counters.duplicates) == (3, 1)
    with sqlite3.connect(repository._db_path) as db:
        rows = db.execute("SELECT source, COUNT(*) FROM radar_events GROUP BY source").fetchall()
        assert rows == [("gdelt", 2), ("rss", 1)]
        # The database, not only the pipeline, still rejects a second RSS row with that URL.
        with pytest.raises(sqlite3.IntegrityError, match="source, radar_events.url"):
            db.execute(
                "INSERT INTO radar_events (id, source, external_id, url, original_text,"
                " normalized_text, content_hash, published_at, discovered_at, status,"
                " created_at, updated_at) SELECT 'copy', source, 'other', url, original_text,"
                " normalized_text, content_hash, published_at, discovered_at, status,"
                " created_at, updated_at FROM radar_events WHERE source = 'rss'"
            )


async def test_upgrade_to_shared_quote_urls_keeps_rows_and_children(tmp_path: Path) -> None:
    from importlib.resources import files

    path = tmp_path / "old.db"
    migrations = sorted(
        item
        for item in files("qmemo_radar.infrastructure.storage.migrations").iterdir()
        if item.name.endswith(".sql") and int(item.name[:3]) <= 8
    )
    with sqlite3.connect(path) as db:
        for migration in migrations:
            db.executescript(migration.read_text(encoding="utf-8"))
    first = build_candidate(article(SourceType.RSS, 1))
    copy = build_candidate(article(SourceType.RSS, 2)).model_copy(
        update={"duplicate_of_event_id": first.event_id}
    )
    with sqlite3.connect(path) as db:
        # Rows as the pre-M3 code wrote them: the current code writes columns 010 adds.
        db.executemany(
            "INSERT INTO radar_events (id, source, external_id, url, original_text,"
            " normalized_text, content_hash, published_at, discovered_at, status, created_at,"
            " updated_at, source_key, duplicate_of_event_id)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'DISCOVERED', 'now', 'now', 'rss:wire', ?)",
            [
                (
                    event.event_id,
                    event.source.value,
                    event.external_id,
                    str(event.url),
                    event.original_text,
                    event.normalized_text,
                    event.content_hash,
                    event.published_at.isoformat(),
                    event.discovered_at.isoformat(),
                    event.duplicate_of_event_id,
                )
                for event in (first, copy)
            ],
        )
        db.execute(
            "INSERT INTO feedback (event_id, action, telegram_user_id, created_at)"
            " VALUES (?, 'SKIP', 1, 'now')",
            (first.event_id,),
        )

    await SQLiteEventRepository(path).initialize()

    with sqlite3.connect(path) as db:  # 009 and 010 both apply to the old database
        assert db.execute("SELECT MAX(version) FROM schema_migrations").fetchone() == (10,)
        assert db.execute(
            "SELECT rowid, id, duplicate_of_event_id FROM radar_events ORDER BY rowid"
        ).fetchall() == [(1, first.event_id, None), (2, copy.event_id, first.event_id)]
        assert db.execute("PRAGMA foreign_key_check").fetchall() == []
        assert db.execute("SELECT event_id FROM feedback").fetchall() == [(first.event_id,)]
        indexes = {row[1] for row in db.execute("PRAGMA index_list(radar_events)")}
    assert {"idx_events_source_url", "idx_events_content_hash", "idx_events_duplicate_of"} <= set(
        indexes
    )


async def test_gdelt_and_rss_run_together_free_while_paid_x_is_blocked(
    repository: SQLiteEventRepository, monkeypatch: pytest.MonkeyPatch
) -> None:
    import gzip
    import json

    from qmemo_radar.application.budget import month_start
    from qmemo_radar.infrastructure.collectors import rss as rss_module

    async def public(host: str, port: int) -> list[str]:
        return ["93.184.216.34"]

    monkeypatch.setattr(rss_module, "_resolve", public)
    now = datetime.now(UTC)
    quote = {
        "date": now.isoformat(),
        "url": "https://news.example/gdelt",
        "title": "Budget",
        "lang": "ENGLISH",
        "quotes": [{"pre": "", "quote": "The budget is final and it will not change", "post": ""}],
    }
    feed = (
        "<rss><channel><item><title>A fresh headline for the radar today</title>"
        f"<link>https://news.example/rss</link><pubDate>{now:%a, %d %b %Y %H:%M:%S} +0000"
        "</pubDate></item></channel></rss>"
    ).encode()
    requests: list[str] = []

    def web(request: httpx.Request) -> httpx.Response:
        requests.append(request.url.host)
        if request.url.host == "data.gdeltproject.org":
            return httpx.Response(200, content=gzip.compress(json.dumps(quote).encode()))
        if request.url.host == "wire.example":
            return httpx.Response(200, content=feed)
        return httpx.Response(599)  # X must never be reached

    # The month's paid budget is already used up.
    spent = CostEntry(
        provider="x", operation="earlier", units=1, estimated_cost_usd=Decimal(10), created_at=now
    )
    assert await repository.reserve_cost(spent, since=month_start(now), limit_usd=Decimal(10))
    guard = BudgetGuard(
        repository,
        enabled=frozenset(PaidFeature),
        hard_limit_usd=Decimal(10),
        target_usd=Decimal(0),
    )
    transport = httpx.MockTransport(web)
    sources = X_SOURCES.model_copy(
        update={
            "rss": SourcesConfig.model_validate(
                {"rss": {"feeds": [{"name": "wire", "url": "https://wire.example/rss"}]}}
            ).rss
        }
    )
    config = settings(
        **X_ON, gdelt_enabled=True, collect_interval_minutes=5, gdelt_max_minutes_per_run=5
    )
    context = SourceContext(
        config,
        sources,
        XApiClient(httpx.AsyncClient(transport=transport), guard=guard),
        free_http=httpx.AsyncClient(transport=transport),
    )
    collector = build_collector(context)
    assert collector.names == ["x_search", "gdelt_gqg", "rss"]

    counters = await pipeline(collector, repository).run_once()

    assert counters.inserted == 2  # the GDELT quote (same in every file) and the RSS entry
    assert counters.source_errors == 2  # the two X queries, blocked before any request
    assert "api.x.com" not in requests
    assert await repository.cost_since(month_start(now)) == Decimal(10)  # nothing added
    with sqlite3.connect(repository._db_path) as db:
        assert db.execute("SELECT COUNT(*) FROM cost_ledger").fetchone() == (1,)


async def test_free_sources_alone_never_touch_the_cost_ledger(
    repository: SQLiteEventRepository, monkeypatch: pytest.MonkeyPatch
) -> None:
    from qmemo_radar.application.budget import month_start
    from qmemo_radar.infrastructure.collectors import rss as rss_module

    async def public(host: str, port: int) -> list[str]:
        return ["93.184.216.34"]

    monkeypatch.setattr(rss_module, "_resolve", public)
    sources = SourcesConfig.model_validate(
        {"rss": {"feeds": [{"name": "wire", "url": "https://wire.example/rss"}]}}
    )
    config = settings(gdelt_enabled=True, collect_interval_minutes=5, gdelt_max_minutes_per_run=5)
    http = httpx.AsyncClient(transport=httpx.MockTransport(lambda request: httpx.Response(404)))
    collector = build_collector(SourceContext(config, sources, x_client=None, free_http=http))

    await pipeline(collector, repository).run_once()

    assert collector.names == ["gdelt_gqg", "rss"]
    assert await repository.cost_since(month_start(datetime.now(UTC))) == Decimal(0)


async def test_a_free_source_backlog_does_not_starve_fresh_candidates(
    repository: SQLiteEventRepository,
) -> None:
    # Regression: only the 100 oldest DISCOVERED events were re-checked and ranked, so a GDELT
    # backlog of thousands kept every new item from reaching the ranker.
    from qmemo_radar.exceptions import RankingFailed

    earlier = datetime.now(UTC) - timedelta(hours=30)
    backlog = [
        build_candidate(article(SourceType.GDELT, n), discovered_at=earlier) for n in range(150)
    ]
    assert await repository.add_events(backlog) == 150
    seen: list[str] = []

    class Recording:
        async def rank(self, events: object) -> list[object]:
            seen.extend(event.external_id for event in events)  # type: ignore[attr-defined]
            raise RankingFailed("provider_down", retryable=True)

    fresh = Feed("rss:wire", [article(SourceType.RSS, 1)])
    radar = pipeline(MultiSourceCollector({"rss": fresh}), repository)
    radar._ranker = Recording()  # type: ignore[assignment]

    await radar.run_once()

    assert seen[0] == "rss-1"


async def test_url_duplicate_lookup_uses_the_partial_index(
    repository: SQLiteEventRepository,
) -> None:
    statements: list[str] = []
    events = [build_candidate(article(SourceType.RSS, 1))]
    original = repository._connect

    def tracing() -> object:
        context = original()

        class Traced:
            async def __aenter__(self) -> object:
                db = await context.__aenter__()
                await db.set_trace_callback(statements.append)
                return db

            async def __aexit__(self, *exc: object) -> None:
                await context.__aexit__(*exc)

        return Traced()

    repository._connect = tracing  # type: ignore[method-assign]
    await repository.find_known(events)
    repository._connect = original  # type: ignore[method-assign]

    [lookup] = [sql for sql in statements if "url IN" in sql]
    with sqlite3.connect(repository._db_path) as db:
        plan = " ".join(str(row[-1]) for row in db.execute(f"EXPLAIN QUERY PLAN {lookup}"))
    assert "idx_events_source_url" in plan

"""Selection end to end: copies become mentions, variants become one story, stories are
preselected and ranked for free, and the best reach Telegram without any paid call."""

import json
import sqlite3
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from fakes import FakeGateway, review_service

from qmemo_radar.application.budget import BudgetGuard, PaidFeature, month_start
from qmemo_radar.application.filtering import FilterPolicy
from qmemo_radar.application.free_ranking import FreeRanker
from qmemo_radar.application.pipeline import PipelineThresholds, RadarPipeline
from qmemo_radar.application.selection import SelectionPolicy
from qmemo_radar.domain import CostEntry, EventStatus, RawSourceItem, SourceFetch, SourceType
from qmemo_radar.exceptions import BudgetBlocked
from qmemo_radar.infrastructure.storage import SQLiteEventRepository
from qmemo_radar.interfaces.cli import execute

NOW = datetime.now(UTC)
CLAIM = "We will cut emissions by half before 2030 and I promise we will get there"
OTHER = "The new bridge across the river opens to traffic on Monday morning"


def quote(text: str, site: str, number: int, *, key: str = "gdelt:gqg") -> RawSourceItem:
    return RawSourceItem(
        source=SourceType.GDELT if key.startswith("gdelt") else SourceType.RSS,
        external_id=f"{site}-{number}",
        url=f"https://{site}/story/{number}",
        original_text=text,
        language="ENGLISH",
        published_at=NOW - timedelta(minutes=5),
        source_key=key,
    )


class Batches:
    """A source returning the next prepared batch on every run."""

    def __init__(self, *batches: list[RawSourceItem]) -> None:
        self.batches = list(batches)

    async def collect(self, checkpoints: Mapping[str, str]) -> list[SourceFetch]:
        items = self.batches.pop(0) if len(self.batches) > 1 else self.batches[0]
        by_key: dict[str, list[RawSourceItem]] = {}
        for item in items:
            by_key.setdefault(item.source_key or "x", []).append(item)
        return [SourceFetch(source_key=key, items=tuple(values)) for key, values in by_key.items()]


def radar(
    repository: SQLiteEventRepository, source: object, *, clock: datetime = NOW
) -> RadarPipeline:
    policy = SelectionPolicy()
    return RadarPipeline(
        collector=source,  # type: ignore[arg-type]
        ranker=FreeRanker(repository, policy, clock=lambda: clock),
        repository=repository,
        filter_policy=FilterPolicy(max_age=timedelta(hours=1)),
        thresholds=PipelineThresholds(),
        selection=policy,
        clock=lambda: clock,
    )


def query(repository: SQLiteEventRepository, sql: str) -> list[tuple[object, ...]]:
    with sqlite3.connect(repository._db_path) as db:
        return db.execute(sql).fetchall()


async def test_a_hundred_copies_are_one_row_with_their_provenance(
    repository: SQLiteEventRepository,
) -> None:
    copies = [quote(CLAIM, f"site{n % 40}.example", n) for n in range(100)]
    counters = await radar(repository, Batches(copies)).run_once()

    assert (counters.collected, counters.inserted, counters.duplicates) == (100, 1, 99)
    assert query(repository, "SELECT COUNT(*) FROM content_mentions") == [(99,)]
    [(mentions, domains, members, first, last)] = query(
        repository,
        "SELECT mention_count, domain_count, member_count, first_seen_at, last_seen_at "
        "FROM event_clusters",
    )
    assert (mentions, domains, members) == (100, 40, 1)
    assert first == last == NOW.isoformat()
    # The one row holds the payload once; the copies are URL, domain and source only.
    assert query(repository, "SELECT COUNT(*) FROM radar_events") == [(1,)]

    again = await radar(repository, Batches(copies)).run_once()
    assert (again.inserted, again.duplicates) == (0, 100)
    assert query(repository, "SELECT COUNT(*) FROM content_mentions") == [(99,)]
    assert query(repository, "SELECT mention_count FROM event_clusters") == [(100,)]


async def test_variants_join_one_story_and_different_statements_stay_apart(
    repository: SQLiteEventRepository,
) -> None:
    first = [
        quote("CEO says Bitcoin will reach $200k this year", "a.example", 1),
        quote("CEO says Bitcoin will reach $100k this year", "b.example", 2),
        quote("CEO says Ethereum will reach $200k this year", "c.example", 3),
    ]
    later = [
        quote("CEO: Bitcoin could hit $200,000 this year", "d.example", 4),
        quote("CEO says Bitcoin will reach $200k this year!", "e.example", 5),
        quote("Le PDG dit que Bitcoin atteindra 200k cette annee", "f.example", 6),
    ]
    later[-1] = later[-1].model_copy(update={"language": "FRENCH"})
    source = Batches(first, later)

    await radar(repository, source).run_once()
    await radar(repository, source, clock=NOW + timedelta(minutes=20)).run_once()

    stories = dict(
        query(
            repository,
            "SELECT e.external_id, c.member_count FROM event_clusters c "
            "JOIN radar_events e ON e.id = c.representative_event_id",
        )
    )
    # $100k, Ethereum and the French text are other statements; both variants join a.example.
    assert stories == {"a.example-1": 3, "b.example-2": 1, "c.example-3": 1, "f.example-6": 1}
    members = dict(
        query(
            repository,
            "SELECT external_id, filter_reason FROM radar_events WHERE filter_reason IS NOT NULL",
        )
    )
    assert members == {"d.example-4": "near_duplicate", "e.example-5": "near_duplicate"}
    metrics = await repository.metrics_since(NOW - timedelta(days=1))
    assert metrics["gdelt:gqg"]["near_duplicates"] == 2
    assert metrics["gdelt:gqg"]["clusters_created"] == 4


async def test_free_ranking_takes_a_widely_reported_story_to_telegram_at_no_cost(
    repository: SQLiteEventRepository,
) -> None:
    wide = [quote(CLAIM, f"outlet{n}.example", n) for n in range(12)]
    wide += [quote(CLAIM, "wire.example", 1, key="rss:wire")]
    lone = [quote(OTHER, "local.example", 99)]
    # The paid budget is exhausted: nothing on the free path may notice.
    guard = BudgetGuard(
        repository,
        enabled=frozenset({PaidFeature.LLM}),
        hard_limit_usd=Decimal("0.01"),
        target_usd=Decimal(0),
    )
    await repository.reserve_cost(
        CostEntry(
            provider="llm",
            operation="rank",
            units=1,
            estimated_cost_usd=Decimal("0.01"),
            created_at=datetime.now(UTC),
        ),
        since=month_start(datetime.now(UTC)),
        limit_usd=Decimal("0.01"),
    )
    with pytest.raises(BudgetBlocked):
        await guard.reserve(
            PaidFeature.LLM, provider="llm", operation="rank", units=1, cost_usd=Decimal("0.001")
        )
    spent = await repository.cost_since(month_start(datetime.now(UTC)))

    counters = await radar(repository, Batches(wide + lone)).run_once()

    statuses = dict(
        query(
            repository,
            "SELECT e.original_text, e.status FROM radar_events e "
            "JOIN event_clusters c ON c.representative_event_id = e.id",
        )
    )
    assert statuses == {CLAIM: "SHORTLISTED", OTHER: "DISCOVERED"}  # one site: not preselected
    assert (counters.scored, counters.shortlisted) == (1, 1)
    gateway = FakeGateway()
    assert await review_service(repository, gateway).deliver(urgent=False) == 1
    [(card, _, _)] = gateway.cards
    assert card.score.model_name == "none" and card.score.prompt_version == "free-rank-v1"
    assert "13 статьях на 13 сайтах" in card.score.summary
    assert card.score.fact_check_required
    assert await repository.cost_since(month_start(datetime.now(UTC))) == spent  # nothing added


async def test_one_article_gives_one_preselected_quote_and_reprints_count_once(
    repository: SQLiteEventRepository,
) -> None:
    def reprint(text: str, site: str, number: int, title: str) -> RawSourceItem:
        return quote(text, site, number).model_copy(update={"raw_payload": {"title": title}})

    wire = "Wire: storm closes schools"
    # One wire article with two quotes, reprinted by 20 local sites under the same title.
    syndicated = [
        reprint(text, f"local{n}.example", n * 10 + i, wire)
        for n in range(20)
        for i, text in enumerate((CLAIM, OTHER))
    ]
    # The same claim quoted by 5 independent articles.
    independent = [reprint(CLAIM, f"paper{n}.example", n, f"Paper {n} headline") for n in range(5)]

    await radar(repository, Batches(syndicated)).run_once()
    stories = dict(
        query(
            repository,
            "SELECT e.original_text, c.domain_count || '/' || c.article_count || '/' || c.state "
            "FROM event_clusters c JOIN radar_events e ON e.id = c.representative_event_id",
        )
    )
    # 20 sites but one article: not notable enough on its own, and the two quotes are one piece.
    assert stories == {CLAIM: "20/1/candidate", OTHER: "20/1/candidate"}

    await radar(repository, Batches(independent), clock=NOW + timedelta(minutes=10)).run_once()
    [(counts, state)] = query(
        repository,
        "SELECT c.domain_count || '/' || c.article_count, c.state FROM event_clusters c "
        "JOIN radar_events e ON e.id = c.representative_event_id "
        f"WHERE e.original_text = '{CLAIM}'",
    )
    assert (counts, state) == ("25/6", "preselected")


async def test_a_second_quote_of_a_preselected_article_is_its_sibling(
    repository: SQLiteEventRepository,
) -> None:
    items = []
    for n in range(10):  # ten independent articles, each quoting the same two statements
        for i, text in enumerate((CLAIM, OTHER)):
            items.append(
                quote(text, f"paper{n}.example", n * 10 + i).model_copy(
                    update={"raw_payload": {"title": f"Paper {n} headline"}}
                )
            )

    await radar(repository, Batches(items)).run_once()

    states = dict(
        query(
            repository,
            "SELECT e.original_text, c.state FROM event_clusters c "
            "JOIN radar_events e ON e.id = c.representative_event_id",
        )
    )
    # Both are well covered; the reader gets the stronger quote of the article, once.
    assert states == {CLAIM: "preselected", OTHER: "sibling"}
    metrics = await repository.metrics_since(NOW - timedelta(days=1))
    assert metrics["gdelt:gqg"]["article_siblings"] == 1


async def test_a_story_ranked_low_is_ranked_again_when_it_grows(
    repository: SQLiteEventRepository,
) -> None:
    early = [quote(CLAIM, f"early{n}.example", n) for n in range(3)]  # preselected: 60 < 65
    grown = [quote(CLAIM, f"late{n}.example", n) for n in range(30)]
    source = Batches(early, grown)

    await radar(repository, source).run_once()
    first = query(repository, "SELECT status FROM radar_events")
    await radar(repository, source, clock=NOW + timedelta(minutes=30)).run_once()

    assert first == [(EventStatus.ARCHIVED.value,)]
    assert query(repository, "SELECT status FROM radar_events") == [
        (EventStatus.SHORTLISTED.value,)
    ]
    metrics = await repository.metrics_since(NOW - timedelta(days=1))
    assert metrics["selection"]["reopened"] == 1


async def test_template_texts_sharing_band_keys_stay_bounded(
    repository: SQLiteEventRepository, monkeypatch: pytest.MonkeyPatch
) -> None:
    from qmemo_radar.application import pipeline as module

    monkeypatch.setattr(module, "MAX_STORIES_PER_KEY", 3)
    scores = [
        quote(f"The home team won the match {n} to {n + 1} on Saturday", "s.example", n)
        for n in range(10)
    ]

    await radar(repository, Batches(scores)).run_once()

    # Different numbers: ten stories. Each band key indexes at most three of them.
    assert query(repository, "SELECT COUNT(*) FROM event_clusters") == [(10,)]
    [(fullest,)] = query(
        repository, "SELECT MAX(n) FROM (SELECT COUNT(*) n FROM cluster_keys GROUP BY band_key)"
    )
    assert fullest == 3
    metrics = await repository.metrics_since(NOW - timedelta(days=1))
    assert metrics["gdelt:gqg"]["band_keys_full"] >= 1


async def test_audit_commands_show_the_funnel_stories_and_rejections(
    repository: SQLiteEventRepository,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(repository._db_path.parent)
    items = [quote(CLAIM, f"outlet{n}.example", n) for n in range(5)]
    items += [quote("Gulf of America", "short.example", 1), quote(OTHER, "one.example", 2)]
    await radar(repository, Batches(items)).run_once()
    db = repository._db_path

    assert await execute("funnel", db_path=db) == 0
    funnel = json.loads(capsys.readouterr().out)
    assert funnel["flow"]["collected"] == 7
    assert funnel["flow"]["rejected_by_reason"] == {"too_short": 1}
    assert funnel["flow"]["exact_copies_kept_as_mentions"] == 4
    assert funnel["stored_now"]["clusters_by_state"] == {"preselected": 1, "candidate": 1}
    assert funnel["cost_month_usd"] == "0"

    assert await execute("clusters", db_path=db, limit=5) == 0
    listed = json.loads(capsys.readouterr().out)["clusters"]
    assert [row["domain_count"] for row in listed] == [5, 1]

    assert await execute("cluster", db_path=db, target=str(listed[0]["id"])) == 0
    story = json.loads(capsys.readouterr().out)["cluster"]
    assert len(story["domains"]) == 5 and len(story["mentions"]) == 5
    assert story["preselect"]["notes"]  # why it passed

    assert await execute("rejected", db_path=db, reason="too_short") == 0
    [sample] = json.loads(capsys.readouterr().out)["samples"]
    assert sample["text"] == "Gulf of America" and sample["url"] == "https://short.example/story/1"

    assert await execute("cluster", db_path=db, target="nope") == 2
    assert await execute("funnel", db_path=Path(db.parent / "missing.db")) == 1


async def test_stories_of_a_run_that_failed_before_scoring_are_scored_next_run(
    repository: SQLiteEventRepository, monkeypatch: pytest.MonkeyPatch
) -> None:
    wide = [quote(CLAIM, f"outlet{n}.example", n) for n in range(12)]
    source = Batches(wide, [])
    original = repository.save_preselection

    async def crash(scores: object) -> None:
        raise RuntimeError("disk full")

    monkeypatch.setattr(repository, "save_preselection", crash)
    with pytest.raises(RuntimeError):
        await radar(repository, source).run_once()
    assert query(repository, "SELECT state, preselect_score FROM event_clusters") == [
        ("candidate", -1)
    ]

    monkeypatch.setattr(repository, "save_preselection", original)
    await radar(repository, source).run_once()  # nothing new arrives

    [(state, score)] = query(repository, "SELECT state, preselect_score FROM event_clusters")
    assert state == "preselected" and score >= 65
    assert query(repository, "SELECT status FROM radar_events") == [("SHORTLISTED",)]


async def test_a_preselected_story_that_cools_down_keeps_its_place(
    repository: SQLiteEventRepository,
) -> None:
    wide = [quote(CLAIM, f"outlet{n}.example", n) for n in range(12)]
    later = NOW + timedelta(hours=30)
    late = [  # one more copy, a day later
        quote(CLAIM, "late.example", 99).model_copy(
            update={"published_at": later - timedelta(minutes=5)}
        )
    ]
    source = Batches(wide, late)

    await radar(repository, source).run_once()
    await radar(repository, source, clock=later).run_once()

    [(state, score)] = query(repository, "SELECT state, preselect_score FROM event_clusters")
    # The score fell (freshness, momentum), the state did not: it was ranked and holds the article.
    assert state == "preselected" and score < 65
    assert query(repository, "SELECT status FROM radar_events") == [("SHORTLISTED",)]


async def test_real_gdelt_and_rss_collectors_reach_telegram_through_free_ranking(
    repository: SQLiteEventRepository,
) -> None:
    from test_gdelt import NOW as GDELT_NOW
    from test_gdelt import Gdelt, gz, minute
    from test_rss import RSS, Web, ok

    from qmemo_radar.application.collection import MultiSourceCollector
    from qmemo_radar.bootstrap import selection_policy
    from qmemo_radar.config import SourcesConfig

    statement = "We will cut emissions by half before 2030 and we will not step back from it"
    articles = [
        {
            "date": "2026-09-16T12:05:59Z",
            "url": f"https://paper{n}.example/climate",
            "title": f"Paper {n}: the minister on climate",
            "lang": "ENGLISH",
            "quotes": [{"pre": "The minister said ", "quote": statement, "post": "."}],
        }
        for n in range(8)
    ]
    gdelt = Gdelt({minute("12:05"): gz(*articles)}).collector()
    rss = Web({"https://wire.example/rss": ok(RSS)}).collector(("wire", "https://wire.example/rss"))
    policy = selection_policy(SourcesConfig().selection)
    pipeline = RadarPipeline(
        collector=MultiSourceCollector({"gdelt_gqg": gdelt, "rss": rss}),
        ranker=FreeRanker(repository, policy, clock=lambda: GDELT_NOW),
        repository=repository,
        filter_policy=FilterPolicy(max_age=timedelta(days=36500)),
        thresholds=PipelineThresholds(),
        selection=policy,
        clock=lambda: GDELT_NOW,
    )

    counters = await pipeline.run_once()

    assert counters.source_errors == 0 and counters.shortlisted == 1
    [(text, status, articles_count)] = query(
        repository,
        "SELECT e.original_text, e.status, c.article_count FROM event_clusters c "
        "JOIN radar_events e ON e.id = c.representative_event_id WHERE e.status = 'SHORTLISTED'",
    )
    assert (text, articles_count) == (statement, 8)
    # The RSS entries were stored and clustered too; one outlet each is not enough to rank.
    assert query(repository, "SELECT COUNT(*) FROM radar_events WHERE source = 'rss'") == [(2,)]
    gateway = FakeGateway()
    assert await review_service(repository, gateway).deliver(urgent=False) == 1
    assert gateway.cards[0][0].event.original_text == statement
    assert await repository.cost_since(month_start(datetime.now(UTC))) == Decimal(0)


async def _crash_refresh_once(
    repository: SQLiteEventRepository, monkeypatch: pytest.MonkeyPatch, source: Batches
) -> None:
    original = repository.refresh_clusters

    async def crash(*args: object, **kwargs: object) -> None:
        raise RuntimeError("disk full")

    monkeypatch.setattr(repository, "refresh_clusters", crash)
    with pytest.raises(RuntimeError):
        await radar(repository, source).run_once()
    monkeypatch.setattr(repository, "refresh_clusters", original)


async def test_copies_stored_by_a_failed_run_still_reach_the_story(
    repository: SQLiteEventRepository, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Independent review: aggregates stayed at 1 site forever after this crash.
    copies = [quote(CLAIM, f"s{n}.example", 10 + n) for n in range(5)]
    source = Batches([quote(CLAIM, "a.example", 1)], copies, copies, [])
    await radar(repository, source).run_once()
    await _crash_refresh_once(repository, monkeypatch, source)
    await radar(repository, source).run_once()  # the same copies again: already known
    await radar(repository, source).run_once()

    assert query(repository, "SELECT mention_count, domain_count FROM event_clusters") == [(6, 6)]


async def test_a_variant_joined_by_a_failed_run_still_reaches_the_story(
    repository: SQLiteEventRepository, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = Batches([quote(CLAIM, "a.example", 1)], [quote(CLAIM + " today", "b.example", 2)], [])
    await radar(repository, source).run_once()
    await _crash_refresh_once(repository, monkeypatch, source)
    await radar(repository, source).run_once()

    assert query(repository, "SELECT member_count, domain_count FROM event_clusters") == [(2, 2)]


async def test_a_copy_filtered_for_its_age_does_not_own_the_fresh_copies(
    repository: SQLiteEventRepository,
) -> None:
    # Independent review: one 3-hour-old copy turned 30 fresh copies into its mentions, and the
    # story was never clustered or ranked.
    old = quote(CLAIM, "old.example", 1).model_copy(
        update={"published_at": NOW - timedelta(hours=3)}
    )
    fresh = [quote(CLAIM, f"s{n}.example", 10 + n) for n in range(30)]

    await radar(repository, Batches([old, *fresh])).run_once()

    rows = query(
        repository, "SELECT external_id, status, filter_reason FROM radar_events ORDER BY rowid"
    )
    assert rows == [
        ("old.example-1", "FILTERED_OUT", "too_old"),
        ("s0.example-10", "SHORTLISTED", None),
    ]
    assert query(repository, "SELECT mention_count FROM event_clusters") == [(30,)]


async def test_stories_are_refreshed_in_bounded_batches(
    repository: SQLiteEventRepository, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Independent review: every touched story was loaded at once (131 MB for 20k stories).
    from qmemo_radar.application import pipeline as module

    monkeypatch.setattr(module, "SCORE_BATCH_SIZE", 40)
    sizes: list[int] = []
    original = repository.refresh_clusters

    async def spy(cluster_ids: list[int], *, now: datetime) -> object:
        sizes.append(len(cluster_ids))
        return await original(cluster_ids, now=now)

    monkeypatch.setattr(repository, "refresh_clusters", spy)
    items = [
        quote(f"Statement number {n} about a completely separate local matter here", "s.example", n)
        for n in range(100)
    ]
    await radar(repository, Batches(items)).run_once()

    assert sizes == [40, 40, 20]
    assert query(repository, "SELECT COUNT(*) FROM event_clusters WHERE preselect_score = -1") == [
        (0,)
    ]


def test_the_token_hash_cache_holds_bytes_and_is_bounded() -> None:
    from qmemo_radar.application.selection import _token_digest

    assert isinstance(_token_digest("bitcoin"), bytes) and len(_token_digest("bitcoin")) == 128
    assert _token_digest.cache_info().maxsize == 100_000

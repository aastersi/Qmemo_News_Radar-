import sqlite3
import time
from datetime import UTC, datetime, timedelta

from fakes import x_item

from qmemo_radar.application.filtering import FilterPolicy
from qmemo_radar.application.pipeline import PipelineThresholds, RadarPipeline
from qmemo_radar.application.ports import PruneCutoffs
from qmemo_radar.infrastructure.collectors import FakeCollector
from qmemo_radar.infrastructure.ranking import DeterministicFixtureRanker
from qmemo_radar.infrastructure.storage import SQLiteEventRepository


def everything_older_than(moment: datetime) -> PruneCutoffs:
    return PruneCutoffs(
        noise_before=moment, evidence_before=moment, index_before=moment, metrics_before=moment
    )


def table_sizes(repository: SQLiteEventRepository) -> dict[str, int]:
    with sqlite3.connect(repository._db_path) as db:
        return {
            table: db.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            for table in (
                "radar_events",
                "content_mentions",
                "event_clusters",
                "cluster_keys",
                "rejected_samples",
                "event_scores",
            )
        }


async def test_prune_counts_first_and_never_deletes_what_a_person_touched(
    repository: SQLiteEventRepository,
) -> None:
    copied = "A plain roadmap update that nobody quoted anywhere this week."
    plain = "Another plain roadmap update with no quotable sentence."
    reacted = "A third plain update that the owner still reacted to."
    items = [
        x_item(1, "An old statement from long before this collection.", minutes_ago=600),
        x_item(2, copied),  # ARCHIVED story
        x_item(3, copied),  # an exact copy: a mention of 1002
        x_item(4, plain),  # ARCHIVED story ...
        x_item(6, plain.replace(".", " today.")),  # ... and its stored variant
        x_item(5, reacted),  # ARCHIVED, and the owner gave feedback
        x_item(7, reacted.replace(".", " today.")),  # a variant of the story the owner saw
        x_item(8, "Too short"),  # rejected before storage: only a sample
    ]
    await RadarPipeline(
        collector=FakeCollector(items),
        ranker=DeterministicFixtureRanker(),
        repository=repository,
        filter_policy=FilterPolicy(max_age=timedelta(hours=1)),
        thresholds=PipelineThresholds(archive=50, digest=90, urgent=95),
    ).run_once()
    with sqlite3.connect(repository._db_path) as db:
        db.execute(
            "INSERT INTO feedback (event_id, action, telegram_user_id, created_at) "
            "SELECT id, 'SKIP', 1, '2026-01-01' FROM radar_events WHERE external_id = '1005'"
        )
        stored = dict(
            db.execute("SELECT external_id, COALESCE(filter_reason, status) FROM radar_events")
        )
    assert stored == {
        "1001": "too_old",
        "1002": "ARCHIVED",
        "1004": "ARCHIVED",
        "1005": "ARCHIVED",
        "1006": "near_duplicate",
        "1007": "near_duplicate",
    }
    before = table_sizes(repository)
    with sqlite3.connect(repository._db_path) as db:
        [(metric_rows,)] = db.execute("SELECT COUNT(*) FROM pipeline_metrics").fetchall()
    later = everything_older_than(datetime.now(UTC) + timedelta(days=1))

    counted = await repository.prune(later, apply=False)
    assert table_sizes(repository) == before  # a dry run changes nothing
    assert await repository.prune(
        everything_older_than(datetime.now(UTC) - timedelta(days=14)), apply=False
    ) == dict.fromkeys(counted, 0)

    deleted = await repository.prune(later, apply=True)
    assert (
        deleted
        == counted
        == {
            "duplicate_texts": 1,  # 1006; 1007 belongs to the story the owner saw
            "copy_mentions": 1,  # 1003 seen at another URL
            "rejected_samples": 1,
            "stories": 2,  # of 1002 and 1004
            "noise_events": 3,  # 1001, 1002, 1004
            "band_keys": 24,  # of the three stories (8 each), all older than the cutoff
            "metrics": metric_rows,  # flow counters of the run, older than the cutoff
            "runs": 0,  # run_once alone records no pipeline_runs row
        }
    )
    with sqlite3.connect(repository._db_path) as db:
        left = sorted(row[0] for row in db.execute("SELECT external_id FROM radar_events"))
        story = db.execute(
            "SELECT e.external_id FROM event_clusters c "
            "JOIN radar_events e ON e.id = c.representative_event_id"
        ).fetchall()
        keys = db.execute("SELECT COUNT(DISTINCT cluster_id) FROM cluster_keys").fetchone()[0]
    assert left == ["1005", "1007"]
    # The story the owner saw stays; its band keys are an index and go past the window.
    assert story == [("1005",)] and keys == 0
    assert await repository.prune(later, apply=True) == dict.fromkeys(counted, 0)


async def test_prune_stays_fast_on_a_large_table(repository: SQLiteEventRepository) -> None:
    rows = [
        (
            f"e{n}",
            "rss",
            str(n),
            f"https://news.example/{n}",
            "noise",
            "noise",
            f"h{n}",
            "2026-01-01T00:00:00+00:00",
            "2026-01-01T00:00:00+00:00",
            "FILTERED_OUT",
            "2026-01-01",
            "2026-01-01",
            f"e{n - 1}" if n % 10 == 0 else None,  # every tenth row is a copy of the previous one
        )
        for n in range(1, 30_001)
    ]
    with sqlite3.connect(repository._db_path) as db:
        db.executemany(
            "INSERT INTO radar_events (id, source, external_id, url, original_text, "
            "normalized_text, content_hash, published_at, discovered_at, status, created_at, "
            "updated_at, duplicate_of_event_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            rows,
        )

    started = time.perf_counter()
    counts = await repository.prune(
        everything_older_than(datetime(2026, 2, 1, tzinfo=UTC)), apply=False
    )
    elapsed = time.perf_counter() - started

    assert counts["noise_events"] == 27_000  # the 3,000 originals of stored copies are kept
    assert elapsed < 5  # without the 008 indexes this took minutes (a scan per candidate)


async def test_the_prune_count_never_waits_for_the_write_lock(
    repository: SQLiteEventRepository,
) -> None:
    # Independent review: the dry run deleted and rolled back inside BEGIN IMMEDIATE, holding
    # the write lock longer than a collection waits for it.
    writer = sqlite3.connect(repository._db_path, timeout=0)
    writer.execute("BEGIN IMMEDIATE")  # a collection writing right now
    try:
        started = time.perf_counter()
        counts = await repository.prune(
            everything_older_than(datetime.now(UTC) + timedelta(days=1)), apply=False
        )
        assert time.perf_counter() - started < 2
    finally:
        writer.rollback()
        writer.close()
    assert counts["noise_events"] == 0


async def test_the_prune_count_matches_the_deletion_when_variants_have_copies(
    repository: SQLiteEventRepository,
) -> None:
    # Measured on the real replay: copies of a story variant went with the variant, so the
    # count (165,902 deleted) was 21,211 too high.
    plain = "Another plain roadmap update with no quotable sentence."
    variant = plain.replace(".", " today.")
    await RadarPipeline(
        collector=FakeCollector([x_item(4, plain), x_item(6, variant), x_item(9, variant)]),
        ranker=DeterministicFixtureRanker(),
        repository=repository,
        filter_policy=FilterPolicy(max_age=timedelta(hours=1)),
        thresholds=PipelineThresholds(archive=50, digest=90, urgent=95),
    ).run_once()
    assert table_sizes(repository)["content_mentions"] == 1  # 1009 is a copy of variant 1006
    later = everything_older_than(datetime.now(UTC) + timedelta(days=1))

    counted = await repository.prune(later, apply=False)
    deleted = await repository.prune(later, apply=True)

    assert counted == deleted
    assert (counted["duplicate_texts"], counted["copy_mentions"]) == (1, 0)

"""Synthetic ingestion load: 200,000 items in every CI run, 1,000,000 on request.

Measured: collect -> gate -> normalize -> dedup -> SQLite -> checkpoint -> metrics -> clustering
-> preselection. Ranking is off. The synthetic texts differ only by numbers, the worst case for
clustering (they all share band keys), so this also bounds that stage.
"""

import json
import os
import time
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from qmemo_radar.application.filtering import FilterPolicy
from qmemo_radar.application.pipeline import PipelineThresholds, RadarPipeline
from qmemo_radar.domain import PipelineCounters, RawSourceItem, SourceFetch, SourceType
from qmemo_radar.infrastructure.storage import SQLiteEventRepository

RUN_SIZE = 10_000  # items per collection run, split over four sources
KEYS = (
    ("rss:wire", SourceType.RSS),
    ("rss:blogs", SourceType.RSS),
    ("gdelt:gqg", SourceType.GDELT),
    ("bluesky:jetstream", SourceType.BLUESKY),
)


def synthetic(index: int, published_at: datetime) -> tuple[str, RawSourceItem]:
    """Per 100 items: 97 unique, 1 too short, 1 identical-text copy, 1 re-sent item."""
    base = index - 50 if index % 100 == 99 else index  # re-sent: same source and id as before
    key, source = KEYS[base % len(KEYS)]
    speaker = index - 40 if index % 100 == 97 else base  # a copy repeats an earlier text
    text = f'Speaker {speaker} said: "Statement number {speaker} for the synthetic benchmark."'
    if index % 100 == 98:
        text = "Too short"
    return key, RawSourceItem(
        source=source,
        external_id=str(base),
        url=f"https://bench.example/{source.value}/{base}",
        original_text=text,
        published_at=published_at,
        source_key=key,
    )


class SyntheticSources:
    def __init__(self) -> None:
        self.start = 0

    async def collect(self, checkpoints: Mapping[str, str]) -> list[SourceFetch]:
        published_at = datetime.now(UTC) - timedelta(minutes=5)
        grouped: dict[str, list[RawSourceItem]] = {key: [] for key, _ in KEYS}
        for index in range(self.start, self.start + RUN_SIZE):
            key, item = synthetic(index, published_at)
            grouped[key].append(item)
        cursor = str(self.start + RUN_SIZE)
        return [
            SourceFetch(source_key=key, items=tuple(items), cursor=cursor)
            for key, items in grouped.items()
        ]


async def ingest(total: int, db: Path) -> dict[str, float]:
    repository = SQLiteEventRepository(db)
    await repository.initialize()
    sources = SyntheticSources()
    pipeline = RadarPipeline(
        collector=sources,
        ranker=None,
        repository=repository,
        filter_policy=FilterPolicy(max_age=timedelta(hours=1)),
        thresholds=PipelineThresholds(),
    )
    totals = PipelineCounters()
    started = time.perf_counter()
    for start in range(0, total, RUN_SIZE):
        sources.start = start
        counters = await pipeline.run_once()
        for name in ("collected", "inserted", "duplicates", "filtered", "source_errors"):
            setattr(totals, name, getattr(totals, name) + getattr(counters, name))
    elapsed = time.perf_counter() - started

    runs = total // RUN_SIZE
    assert totals.collected == total and totals.source_errors == 0
    # Re-sent items are never stored twice; an identical-text copy is a mention, not a row;
    assert totals.inserted == total - runs * 300  # and "Too short" is never stored
    assert totals.duplicates == runs * 200  # re-sent items and identical-text copies
    assert totals.filtered == runs * 100
    stored = await repository.count_by_status()
    assert sum(stored.values()) == totals.inserted

    sources.start = 0  # the first run again, e.g. after a lost checkpoint
    again = await pipeline.run_once()
    # The re-sent run: every item is known again, except "Too short", rejected again by the gate.
    assert again.inserted == 0 and again.duplicates == RUN_SIZE - 100

    metrics = await repository.metrics_since(datetime(2000, 1, 1, tzinfo=UTC))
    assert sum(values.get("collected", 0) for values in metrics.values()) == total + RUN_SIZE
    result = {
        "items": total,
        "seconds": round(elapsed, 1),
        "items_per_second": round(total / elapsed),
        "db_megabytes": round(db.stat().st_size / 1_000_000),  # noqa: ASYNC240
    }
    print(json.dumps({"ingestion_benchmark": result}))
    return result


async def test_ingests_200k_items_idempotently(tmp_path: Path) -> None:
    result = await ingest(200_000, tmp_path / "bench.db")
    # Storing one item per connection and commit measured ~7.6 ms per item (~130 items/s) on a
    # laptop, batched chunks ~0.05 ms. 50,000 items/day is under one item per second.
    assert result["items_per_second"] > 500


@pytest.mark.benchmark
@pytest.mark.skipif(
    os.environ.get("RADAR_BENCHMARK_1M") != "1", reason="set RADAR_BENCHMARK_1M=1 to run"
)
async def test_ingests_1m_items(tmp_path: Path) -> None:
    result = await ingest(1_000_000, tmp_path / "bench.db")
    assert result["items_per_second"] > 500

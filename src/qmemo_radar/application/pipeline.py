import json
import logging
from collections import Counter, defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import uuid4

from qmemo_radar.application.filtering import (
    MANUAL_SOURCE_KEY,
    FilterPolicy,
    first_filter_reason,
)
from qmemo_radar.application.normalization import build_candidate
from qmemo_radar.application.ports import EventRepository, Ranker, SourceCollector
from qmemo_radar.application.scoring import calculate_total
from qmemo_radar.domain import (
    SHARED_URL_SOURCES,
    EventCandidate,
    EventStatus,
    Metric,
    PipelineCounters,
    RawSourceItem,
    ScoreResult,
)
from qmemo_radar.exceptions import RankingFailed

logger = logging.getLogger(__name__)

DUPLICATE_CONTENT = "duplicate_content"
# One SQLite transaction per chunk: few commits, and the write lock is never held for long.
INGEST_CHUNK_SIZE = 500


@dataclass(frozen=True, slots=True)
class PipelineThresholds:
    archive: int = 50
    digest: int = 65
    urgent: int = 80


class RadarPipeline:
    """One deterministic collection cycle. It never publishes externally."""

    def __init__(
        self,
        *,
        collector: SourceCollector,
        ranker: Ranker | None,
        repository: EventRepository,
        filter_policy: FilterPolicy,
        thresholds: PipelineThresholds,
    ) -> None:
        self._collector = collector
        self._ranker = ranker
        self._repository = repository
        self._filter_policy = filter_policy
        self._thresholds = thresholds

    async def run_once(self, *, run_id: str | None = None) -> PipelineCounters:
        run_id = run_id or uuid4().hex
        metrics: defaultdict[str, Counter[str]] = defaultdict(Counter)
        checkpoints = await self._repository.get_checkpoints()
        for fetch in await self._collector.collect(checkpoints):
            log = {"run_id": run_id, "operation": "collect", "source_key": fetch.source_key}
            stats = metrics[fetch.source_key]
            stats.update(fetch.stats)
            if fetch.error_code:
                stats[Metric.SOURCE_ERRORS] += 1
                # A cursor on a failed fetch is state that must survive it (e.g. failed attempts).
                await self._repository.record_source_result(
                    fetch.source_key, cursor=fetch.cursor, error_code=fetch.error_code
                )
                logger.warning(
                    "source failed",
                    extra={**log, "result": "failed", "error_code": fetch.error_code},
                )
                continue

            stats[Metric.COLLECTED] += len(fetch.items)
            for chunk in _batches(fetch.items, size=INGEST_CHUNK_SIZE):
                await self._ingest(chunk, stats)
            # A storage error above aborts the run: the cursor moves only after every item is saved.
            await self._repository.record_source_result(
                fetch.source_key, cursor=fetch.cursor, error_code=None
            )
            logger.info("source collected", extra={**log, "result": _describe(stats)})

        pending = await self._repository.list_events_by_status(
            EventStatus.DISCOVERED,
            limit=100,
        )
        candidates: list[EventCandidate] = []
        for event in pending:
            # New items were screened on ingest; this re-check covers manual links, items left
            # from older versions and candidates that became too old while waiting.
            reason = first_filter_reason(event, self._filter_policy)
            if reason is None and await self._repository.has_earlier_content_duplicate(event):
                reason = DUPLICATE_CONTENT
            if reason:
                await self._repository.set_status(
                    event.event_id,
                    EventStatus.FILTERED_OUT,
                    expected={EventStatus.DISCOVERED},
                    filter_reason=reason,
                )
                logger.info(
                    "event filtered",
                    extra={
                        "run_id": run_id,
                        "event_id": event.event_id,
                        "operation": "filter",
                        "result": reason,
                    },
                )
                metrics[event.source_key or event.source.value][_metric_for(reason)] += 1
                continue
            candidates.append(event)

        await self._repository.record_metrics(run_id, metrics)
        counters = PipelineCounters()
        for stats in metrics.values():
            counters.collected += stats[Metric.COLLECTED]
            counters.inserted += stats[Metric.INSERTED]
            counters.duplicates += stats[Metric.EXACT_DUPLICATES]
            counters.filtered += stats[Metric.FILTERED]
            counters.source_errors += stats[Metric.SOURCE_ERRORS]

        if self._ranker is None:
            # No free ranker exists yet and paid LLM is off: candidates wait (or expire) unranked.
            return counters
        for batch in _batches(candidates, size=10):
            if not await self._rank(batch, counters, run_id):
                break

        return counters

    async def _rank(
        self,
        batch: list[EventCandidate],
        counters: PipelineCounters,
        run_id: str | None,
    ) -> bool:
        """Score one batch. Returns False when the provider is down and ranking should stop."""
        assert self._ranker is not None
        try:
            by_id = _validate_results(batch, await self._ranker.rank(batch))
        except RankingFailed as exc:
            logger.warning(
                "ranking batch failed",
                extra={
                    "run_id": run_id,
                    "operation": "rank",
                    "result": f"failed_batch={len(batch)}",
                    "error_code": exc.code,
                },
            )
            if exc.retryable:
                # Provider unreachable: events stay DISCOVERED and are retried next run.
                counters.rank_failed += len(batch)
                return False
            if len(batch) > 1:
                # One bad post must not block its neighbours: rank them one by one.
                for event in batch:
                    if not await self._rank([event], counters, run_id):
                        return False
                return True
            # This single post keeps producing invalid answers: never show it, never retry it.
            counters.rank_failed += 1
            await self._repository.set_status(
                batch[0].event_id,
                EventStatus.FILTERED_OUT,
                expected={EventStatus.DISCOVERED},
                filter_reason="rank_invalid_output",
            )
            return True

        for event in batch:
            score = by_id[event.event_id]
            next_status = (
                EventStatus.SHORTLISTED  # a manual link was chosen by a person
                if event.source_key == MANUAL_SOURCE_KEY
                else self._status_for_score(score.total)
            )
            if not await self._repository.save_score_and_status(score, next_status):
                continue  # expired or otherwise moved on while it was being ranked
            logger.info(
                "event scored",
                extra={
                    "run_id": run_id,
                    "event_id": event.event_id,
                    "operation": "rank",
                    "result": f"{next_status.value}:{score.total}",
                },
            )
            counters.scored += 1
            if next_status is EventStatus.SHORTLISTED:
                counters.shortlisted += 1
            else:
                counters.archived += 1
        return True

    async def _ingest(self, items: Sequence[RawSourceItem], stats: Counter[str]) -> None:
        """Stage 1 for one chunk: normalize, screen, drop exact duplicates, insert in one commit.

        Future stages (near dedup, clustering, preselection) read DISCOVERED events after this
        and before ranking; they must not be added here.
        """
        now = datetime.now(UTC)
        events: list[EventCandidate] = []
        for item in items:
            try:
                events.append(_storable(build_candidate(item, discovered_at=now)))
            except ValueError:
                # One bad upstream item must not abort the run: the cursor would never move and
                # every run would fail on it again.
                stats[Metric.INVALID_ITEMS] += 1
        known = await self._repository.find_known(events)
        ids, urls = set(known.ids), set(known.urls)
        owners = dict(known.content_owners)
        rows: list[EventCandidate] = []
        for event in events:
            id_key = (event.source.value, event.external_id)
            url_key = (event.source.value, str(event.url))
            shared_url = event.source in SHARED_URL_SOURCES
            if id_key in ids or (not shared_url and url_key in urls):
                stats[Metric.EXACT_DUPLICATES] += 1
                continue
            ids.add(id_key)
            if not shared_url:
                urls.add(url_key)
            reason = first_filter_reason(event, self._filter_policy, now=now)
            original = None if reason else owners.get(event.content_hash)
            if original:
                reason = DUPLICATE_CONTENT
            else:
                owners.setdefault(event.content_hash, event.event_id)
            if reason:
                # Kept, not dropped: retention prunes noise later and copies count as signal.
                event = event.model_copy(
                    update={
                        "status": EventStatus.FILTERED_OUT,
                        "filter_reason": reason,
                        "duplicate_of_event_id": original,
                    }
                )
                stats[_metric_for(reason)] += 1
            rows.append(event)
        inserted = await self._repository.add_events(rows)
        stats[Metric.INSERTED] += inserted
        # Only a concurrent writer (a manual link) can make this non-zero.
        stats[Metric.EXACT_DUPLICATES] += len(rows) - inserted

    def _status_for_score(self, total: int) -> EventStatus:
        if total >= self._thresholds.digest:
            return EventStatus.SHORTLISTED
        return EventStatus.ARCHIVED


def _storable(event: EventCandidate) -> EventCandidate:
    """Raise UnicodeEncodeError (a ValueError) for text SQLite cannot store, e.g. a lone
    surrogate decoded from a JSON escape."""
    texts = (event.original_text, event.author_handle, event.author_display_name, event.language)
    "".join(text or "" for text in texts).encode()
    json.dumps(event.raw_payload, ensure_ascii=False, default=str).encode()
    return event


def _batches[T](items: Sequence[T], *, size: int) -> list[list[T]]:
    return [list(items[index : index + size]) for index in range(0, len(items), size)]


def _metric_for(reason: str) -> Metric:
    return Metric.EXACT_DUPLICATES if reason == DUPLICATE_CONTENT else Metric.FILTERED


def _describe(stats: Counter[str]) -> str:
    return " ".join(f"{metric}={value}" for metric, value in stats.items() if value)


def _validate_results(
    events: Sequence[EventCandidate],
    results: Sequence[ScoreResult],
) -> dict[str, ScoreResult]:
    expected_ids = {event.event_id for event in events}
    result_ids = [result.event_id for result in results]
    if len(result_ids) != len(set(result_ids)):
        raise RankingFailed("duplicate_event_ids")
    if set(result_ids) != expected_ids:
        raise RankingFailed("unexpected_event_ids")

    validated: dict[str, ScoreResult] = {}
    for result in results:
        calculated = calculate_total(result.breakdown)
        validated[result.event_id] = result.model_copy(update={"total": calculated})
    return validated

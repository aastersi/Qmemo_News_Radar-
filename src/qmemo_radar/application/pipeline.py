import json
import logging
import random
from collections import Counter, defaultdict
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from uuid import uuid4

from qmemo_radar.application.filtering import (
    MANUAL_SOURCE_KEY,
    FilterPolicy,
    first_filter_reason,
)
from qmemo_radar.application.normalization import build_candidate
from qmemo_radar.application.ports import (
    ClusterJoin,
    ClusterScore,
    EventRepository,
    Mention,
    Ranker,
    RejectedSample,
    SourceCollector,
)
from qmemo_radar.application.scoring import calculate_total
from qmemo_radar.application.selection import (
    SelectionPolicy,
    TextFeatures,
    article_of,
    band_keys,
    compare,
    domain_of,
    features,
    gate_reason,
    preselect,
)
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
# Clustering works through new texts in batches; the cap bounds one run after a backlog (the
# rest waits for the next run, or expires).
CLUSTER_BATCH_SIZE = 1_000
MAX_CLUSTERED_PER_RUN = 60_000
# Rejected texts kept per reason and run for `qmemo-radar rejected`; the rest is only counted.
REJECTED_SAMPLES_PER_RUN = 20
RANK_CANDIDATES_PER_RUN = 100
# Stories indexed under one band key. Template-like texts (same words, different numbers) share
# keys; past this a key is not extended, so candidates per new text stay bounded (8 x 50).
MAX_STORIES_PER_KEY = 50
SELECTION_KEY = "selection"


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
        selection: SelectionPolicy | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._collector = collector
        self._ranker = ranker
        self._repository = repository
        self._filter_policy = filter_policy
        self._thresholds = thresholds
        self._selection = selection or SelectionPolicy()
        self._clock = clock

    async def run_once(self, *, run_id: str | None = None) -> PipelineCounters:
        run_id = run_id or uuid4().hex
        now = self._clock()
        metrics: defaultdict[str, Counter[str]] = defaultdict(Counter)
        run = _RunState(now=now)
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
                await self._ingest(chunk, stats, run, fetch.source_key)
            # A storage error above aborts the run: the cursor moves only after every item is saved.
            await self._repository.record_source_result(
                fetch.source_key, cursor=fetch.cursor, error_code=None
            )
            logger.info("source collected", extra={**log, "result": _describe(stats)})

        await self._repository.add_rejected_samples(
            [sample for _, kept in run.samples.values() for sample in kept]
        )
        await self._cluster(metrics, run)
        pending = await self._repository.rank_candidates(limit=RANK_CANDIDATES_PER_RUN)
        candidates: list[EventCandidate] = []
        for event in pending:
            # New items were screened on ingest; this re-check covers manual links and rules
            # changed since. Age is not re-checked: a story still growing is not too old, and
            # the event TTL ends it.
            reason = first_filter_reason(event, self._filter_policy, now=now, check_age=False)
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
            # Rejected by the gate (never stored) or by the filter (stored as FILTERED_OUT).
            counters.filtered += stats[Metric.FILTERED] + sum(
                value for name, value in stats.items() if name.startswith("rejected_")
            )
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

    async def _ingest(
        self,
        items: Sequence[RawSourceItem],
        stats: Counter[str],
        run: "_RunState",
        fetch_key: str,
    ) -> None:
        """Stage 1 for one chunk: gate, normalize, drop exact duplicates, insert in one commit.

        The gate runs before anything is built, so obvious noise costs a counter and at most a
        small sample. An exact copy of a stored text becomes a mention of it, not a second row.
        """
        now = run.now
        kept: list[RawSourceItem] = []
        events: list[EventCandidate] = []
        for item in items:
            if item.source_key != MANUAL_SOURCE_KEY:
                rejected = gate_reason(item, self._selection, now=now)
                if rejected:
                    stats[f"rejected_{rejected}"] += 1
                    run.sample(rejected, item)
                    continue
            try:
                events.append(_storable(build_candidate(item, discovered_at=now)))
                kept.append(item)
            except ValueError:
                # One bad upstream item must not abort the run: the cursor would never move and
                # every run would fail on it again.
                stats[Metric.INVALID_ITEMS] += 1
        known = await self._repository.find_known(events)
        ids, urls = set(known.ids), set(known.urls)
        owners = dict(known.content_owners)
        rows: list[EventCandidate] = []
        mentions: list[Mention] = []
        for item, event in zip(kept, events, strict=True):
            id_key = (event.source.value, event.external_id)
            url_key = (event.source.value, str(event.url))
            shared_url = event.source in SHARED_URL_SOURCES
            if id_key in ids or (not shared_url and url_key in urls):
                stats[Metric.EXACT_DUPLICATES] += 1
                continue
            ids.add(id_key)
            if not shared_url:
                urls.add(url_key)
            url = str(event.url)
            source_key = event.source_key or fetch_key
            reason = first_filter_reason(event, self._filter_policy, now=now)
            original = None if reason else owners.get(event.content_hash)
            if original:
                stats[Metric.EXACT_DUPLICATES] += 1
                if (original, url) not in known.mentions:
                    # Provenance as one light row; the payload of the copy is not stored again.
                    mentions.append(
                        Mention(original, url, domain_of(url), article_of(item), source_key, now)
                    )
                    run.grown.add(original)
                    stats[Metric.MENTIONS_AGGREGATED] += 1
                continue
            owners.setdefault(event.content_hash, event.event_id)
            if reason:
                # Kept, not dropped: retention prunes noise later.
                event = event.model_copy(
                    update={"status": EventStatus.FILTERED_OUT, "filter_reason": reason}
                )
                stats[_metric_for(reason)] += 1
            rows.append(event)
        inserted = await self._repository.add_events(rows, mentions)
        stats[Metric.INSERTED] += inserted
        # Only a concurrent writer (a manual link) can make this non-zero.
        stats[Metric.EXACT_DUPLICATES] += len(rows) - inserted

    async def _cluster(self, metrics: defaultdict[str, Counter[str]], run: "_RunState") -> None:
        """Stage 2: new texts join a story (near duplicate or same event) or start one; every
        story that changed gets a fresh preselection score."""
        policy, now = self._selection, run.now
        touched = await self._repository.clusters_of(sorted(run.grown))
        cache: dict[str, TextFeatures] = {}
        for _ in range(MAX_CLUSTERED_PER_RUN // CLUSTER_BATCH_SIZE):
            batch = await self._repository.unclustered_events(limit=CLUSTER_BATCH_SIZE)
            if not batch:
                break
            found = {event.event_id: features(event.original_text) for event in batch}
            keys = {event_id: band_keys(feats.tokens) for event_id, feats in found.items()}
            stored = await self._repository.representatives(
                [key for values in keys.values() for key in values],
                seen_since=now - policy.window,
            )
            texts = {rep.event_id: rep.text for reps in stored.values() for rep in reps}
            local: defaultdict[int, list[tuple[str, str | None]]] = defaultdict(list)
            new_clusters: list[tuple[EventCandidate, tuple[int, ...]]] = []
            joins: list[ClusterJoin] = []
            for event in batch:
                source = event.source_key or event.source.value
                event_keys = keys[event.event_id]
                options = {
                    (rep.event_id, rep.language)
                    for key in event_keys
                    for rep in stored.get(key, [])
                } | {option for key in event_keys for option in local.get(key, [])}
                best: tuple[bool, float, str, str] | None = None
                for rep_id, rep_language in options:
                    if (rep_language or "").casefold() != (event.language or "").casefold():
                        continue
                    rep_features = cache.get(rep_id) or found.get(rep_id)
                    if rep_features is None:
                        rep_features = cache[rep_id] = features(texts[rep_id])
                    kind, similarity = compare(rep_features, found[event.event_id], policy)
                    if kind is None:
                        continue
                    option = (kind == "near_duplicate", similarity, rep_id, kind)
                    if best is None or option[:2] > best[:2]:
                        best = option
                if best is None:
                    indexed = tuple(
                        key
                        for key in event_keys
                        if len(stored.get(key, ())) + len(local[key]) < MAX_STORIES_PER_KEY
                    )
                    new_clusters.append((event, indexed))
                    for key in indexed:
                        local[key].append((event.event_id, event.language))
                    metrics[source][Metric.CLUSTERS_CREATED] += 1
                    if len(indexed) < len(event_keys):
                        metrics[source]["band_keys_full"] += 1
                else:
                    joins.append(ClusterJoin(event.event_id, best[2], best[3]))
                    name = Metric.NEAR_DUPLICATES if best[0] else "same_event"
                    metrics[source][name] += 1
            await self._repository.save_clustering(new_clusters, joins, now=now)
            touched |= await self._repository.clusters_of([event.event_id for event in batch])

        # Stories of a run that failed after clustering; this run's own are scored below anyway.
        touched |= await self._repository.unscored_clusters(limit=MAX_CLUSTERED_PER_RUN)
        scores: list[ClusterScore] = []
        reopen: list[str] = []
        refreshed = await self._repository.refresh_clusters(sorted(touched), now=now)
        results = {signals.cluster_id: preselect(signals, policy, now=now) for signals in refreshed}
        # One quote per article: an article with ten quotes is one story for the reader. The
        # quote preselected first keeps the place; within a run the best one takes it.
        articles = {signals.cluster_id: article_of(signals.event) for signals in refreshed}
        holders = await self._repository.preselected_articles(sorted(set(articles.values())))
        for signals in sorted(refreshed, key=lambda s: -results[s.cluster_id].total):
            cluster_id = signals.cluster_id
            assert cluster_id is not None
            result = results[cluster_id]
            source = signals.event.source_key or signals.event.source.value
            always = signals.event.source.value in policy.always_rank_sources
            # Once preselected (or a sibling), always: a story that cools down was ranked
            # already, and its article keeps one holder.
            passes = always or result.total >= policy.min_preselect_score
            passes = passes or signals.state in ("preselected", "sibling")
            holder = holders.setdefault(articles[cluster_id], cluster_id) if passes else None
            if result.boilerplate:
                state = "boilerplate"
            elif signals.state == "sibling" or (passes and holder != cluster_id and not always):
                state = "sibling"
                metrics[source]["article_siblings"] += 1
            elif passes:
                state = "preselected"
                metrics[source][Metric.PRESELECTED] += 1
                grown = result.total >= self._thresholds.digest
                if signals.event.status is EventStatus.ARCHIVED and grown:
                    reopen.append(signals.event.event_id)
            else:
                state = "candidate"
            # Explained only where someone will look: below preselection the score is enough.
            details: dict[str, object] = {}
            if state != "candidate":
                details = {
                    "breakdown": result.breakdown.model_dump(),
                    "notes": list(result.notes),
                    "topics": list(result.topics),
                }
            scores.append(ClusterScore(cluster_id, state, result.total, details))
        await self._repository.save_preselection(scores)
        if reopen:
            # The story grew after it was ranked below the digest: rank it again.
            metrics[SELECTION_KEY]["reopened"] += await self._repository.reopen_archived(reopen)

    def _status_for_score(self, total: int) -> EventStatus:
        if total >= self._thresholds.digest:
            return EventStatus.SHORTLISTED
        return EventStatus.ARCHIVED


@dataclass
class _RunState:
    """What one run collects on the way: its clock, stories that grew, rejected samples."""

    now: datetime
    grown: set[str] = field(default_factory=set)
    samples: dict[str, tuple[int, list[RejectedSample]]] = field(default_factory=dict)

    def sample(self, reason: str, item: RawSourceItem) -> None:
        """Reservoir sample: every rejected item of a run has the same chance to be kept."""
        seen, kept = self.samples.get(reason, (0, []))
        sample = RejectedSample(
            reason, item.source_key, item.original_text, str(item.url), self.now
        )
        if len(kept) < REJECTED_SAMPLES_PER_RUN:
            kept.append(sample)
        elif (slot := random.randrange(seen + 1)) < REJECTED_SAMPLES_PER_RUN:
            kept[slot] = sample
        self.samples[reason] = (seen + 1, kept)


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

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Protocol

from qmemo_radar.application.selection import ClusterSignals
from qmemo_radar.domain import (
    CostEntry,
    DeliveryKind,
    Draft,
    DraftText,
    EventCandidate,
    EventStatus,
    FeedbackAction,
    OutboxStatus,
    PipelineCounters,
    PipelineRun,
    PublicationPackage,
    RawSourceItem,
    RunStatus,
    ScoredEvent,
    ScoreResult,
    SourceFetch,
    SourceHealth,
)


class SourceCollector(Protocol):
    async def collect(self, checkpoints: Mapping[str, str]) -> list[SourceFetch]:
        """Fetch each configured source independently; a failed source returns error_code."""
        ...


class PostLookup(Protocol):
    async def lookup_post(self, post_id: str) -> RawSourceItem: ...


class Ranker(Protocol):
    async def rank(self, events: Sequence[EventCandidate]) -> list[ScoreResult]: ...


@dataclass(frozen=True, slots=True)
class KnownEvents:
    """What storage already holds for a batch of candidates."""

    ids: frozenset[tuple[str, str]]  # (source, external_id)
    urls: frozenset[tuple[str, str]]  # (source, url)
    content_owners: Mapping[str, str]  # content_hash -> earliest non-duplicate event id
    # (owner event id, url) of copies already recorded: a re-sent copy is not a new mention.
    mentions: frozenset[tuple[str, str]] = frozenset()


@dataclass(frozen=True, slots=True)
class Mention:
    """One place a stored text was seen: its own URL or an exact copy elsewhere."""

    event_id: str
    url: str
    domain: str
    article: str
    source_key: str | None
    seen_at: datetime


@dataclass(frozen=True, slots=True)
class PruneCutoffs:
    """Retention: rows older than these may go (see SQLiteEventRepository.prune)."""

    noise_before: datetime  # filtered, expired and archived events; rejected samples
    evidence_before: datetime  # stored variants of a story and mentions of exact copies
    # Band keys of stories not seen since (the clustering window): a derived index only.
    index_before: datetime
    # pipeline_metrics and finished pipeline_runs (counters of old runs).
    metrics_before: datetime


@dataclass(frozen=True, slots=True)
class RejectedSample:
    reason: str
    source_key: str | None
    text: str
    url: str
    seen_at: datetime


@dataclass(frozen=True, slots=True)
class Representative:
    """The first text of a stored story: what a new text is compared with."""

    cluster_id: int
    event_id: str
    text: str
    language: str | None


@dataclass(frozen=True, slots=True)
class ClusterJoin:
    event_id: str
    representative_event_id: str
    reason: str  # near_duplicate | same_event


@dataclass(frozen=True, slots=True)
class ClusterScore:
    cluster_id: int
    state: str  # candidate | preselected | sibling | boilerplate
    score: int
    details: Mapping[str, object]


class SelectionRepository(Protocol):
    async def add_rejected_samples(self, samples: Sequence[RejectedSample]) -> None: ...

    async def unclustered_events(self, *, limit: int) -> list[EventCandidate]:
        """DISCOVERED texts of automated sources not in a cluster yet, oldest first."""
        ...

    async def representatives(
        self, band_keys: Sequence[int], *, seen_since: datetime
    ) -> dict[int, list[Representative]]:
        """Stories active since `seen_since` whose representative shares a band key."""
        ...

    async def save_clustering(
        self,
        new_clusters: Sequence[tuple[EventCandidate, Sequence[int]]],
        joins: Sequence[ClusterJoin],
        *,
        now: datetime,
    ) -> int:
        """Create clusters (representative, band keys) and attach members, in one transaction.
        Returns how many members were attached (a member no longer DISCOVERED is skipped)."""
        ...

    async def clusters_of(self, event_ids: Sequence[str]) -> set[int]: ...

    async def unscored_clusters(self, *, limit: int) -> set[int]:
        """Stories to (re)score: new, or grown since their last score (also by a failed run)."""
        ...

    async def refresh_clusters(
        self, cluster_ids: Sequence[int], *, now: datetime
    ) -> list[ClusterSignals]:
        """Recompute aggregates from mentions; returns ClusterSignals per cluster."""
        ...

    async def save_preselection(self, scores: Sequence[ClusterScore]) -> None: ...

    async def preselected_articles(self, articles: Sequence[str]) -> dict[str, int]:
        """Article -> id of the story already preselected for it (one quote per article)."""
        ...

    async def reopen_archived(self, event_ids: Sequence[str]) -> int:
        """ARCHIVED -> DISCOVERED for representatives of stories that grew after ranking."""
        ...

    async def rank_candidates(self, *, limit: int) -> list[EventCandidate]:
        """DISCOVERED manual links, then representatives of preselected stories by score."""
        ...

    async def cluster_signals(
        self, event_ids: Sequence[str], *, now: datetime
    ) -> list[ClusterSignals]:
        """ClusterSignals of stories by representative event id, for the free ranker."""
        ...


class EventRepository(SelectionRepository, Protocol):
    async def initialize(self) -> None: ...

    async def add_event(self, event: EventCandidate) -> bool: ...

    async def find_known(self, events: Sequence[EventCandidate]) -> KnownEvents: ...

    async def add_events(
        self, events: Sequence[EventCandidate], mentions: Sequence[Mention] = ()
    ) -> int:
        """INSERT OR IGNORE all events in one transaction; returns how many rows were new."""
        ...

    async def record_metrics(self, run_id: str, metrics: Mapping[str, Mapping[str, int]]) -> None:
        """Metric names are `Metric` members or a source's own diagnostic counters."""
        ...

    async def metrics_since(self, since: datetime) -> dict[str, dict[str, int]]: ...

    async def list_events_by_status(
        self,
        status: EventStatus,
        *,
        limit: int,
    ) -> list[EventCandidate]: ...

    async def set_status(
        self,
        event_id: str,
        status: EventStatus,
        *,
        expected: set[EventStatus] | None = None,
        filter_reason: str | None = None,
    ) -> bool: ...

    async def save_score_and_status(
        self,
        score: ScoreResult,
        status: EventStatus,
    ) -> bool:
        """Store the score and status if the event is still DISCOVERED."""
        ...

    async def count_by_status(self) -> dict[str, int]: ...

    async def get_checkpoints(self) -> dict[str, str]: ...

    async def record_source_result(
        self,
        source_key: str,
        *,
        cursor: str | None,
        error_code: str | None,
    ) -> None: ...

    async def has_earlier_content_duplicate(self, event: EventCandidate) -> bool: ...


class ReviewRepository(EventRepository, Protocol):
    async def list_deliverable(
        self,
        statuses: set[EventStatus],
        *,
        min_total: int,
        limit: int,
    ) -> list[ScoredEvent]: ...

    async def record_delivery(
        self,
        event_id: str,
        *,
        chat_id: int,
        message_id: int,
        kind: DeliveryKind,
        expected: set[EventStatus],
    ) -> bool:
        """Store the Telegram message id and mark the event NOTIFIED in one transaction."""
        ...

    async def count_deliveries_since(self, since: datetime) -> int: ...

    async def list_delivered_since(self, since: datetime) -> list[ScoredEvent]: ...

    async def list_scored_by_status(
        self, status: EventStatus, *, limit: int
    ) -> list[ScoredEvent]: ...

    async def get_scored_event(self, event_id: str) -> ScoredEvent | None: ...

    async def decide(
        self,
        event_id: str,
        status: EventStatus,
        *,
        expected: set[EventStatus],
        action: FeedbackAction,
        telegram_user_id: int,
    ) -> bool:
        """Change the event status and store feedback atomically; False if the state moved on."""
        ...

    async def expire_events(self, discovered_before: datetime) -> int: ...

    async def requeue_manual(self, event: EventCandidate) -> bool:
        """Mark an already stored but ARCHIVED or FILTERED_OUT post as a manual pick."""
        ...

    async def get_state(self, key: str) -> str | None: ...

    async def set_state(self, key: str, value: str) -> None: ...


class ReviewGateway(Protocol):
    async def send_card(self, card: ScoredEvent, *, urgent: bool) -> int:
        """Send a card and return the Telegram message id; raise DeliveryFailed on failure."""
        ...


class QuotePublisher(Protocol):
    async def publish(self, package: PublicationPackage) -> str: ...


class XPublisher(Protocol):
    async def publish(self, package: PublicationPackage, qmemo_url: str) -> str: ...


class DraftWriter(Protocol):
    async def write(
        self,
        card: ScoredEvent,
        *,
        previous: Draft | None = None,
        instruction: str | None = None,
    ) -> DraftText:
        """Write a draft, or a revision of `previous`; raise DraftFailed when impossible."""
        ...


class DraftRepository(ReviewRepository, Protocol):
    async def get_draft(self, draft_id: str) -> Draft | None: ...

    async def latest_draft(self, event_id: str) -> Draft | None: ...

    async def latest_revisable_draft(self) -> Draft | None: ...

    async def save_first_draft(self, draft: Draft, *, telegram_user_id: int) -> bool:
        """Insert version 1 and move the event to DRAFTED atomically."""
        ...

    async def save_revision(
        self,
        draft: Draft,
        *,
        previous_id: str,
        action: FeedbackAction,
        telegram_user_id: int,
    ) -> bool:
        """Insert version 2 and supersede the previous version atomically."""
        ...

    async def mark_verified(self, draft_id: str, *, telegram_user_id: int) -> bool: ...

    async def reject_draft(self, draft: Draft, *, telegram_user_id: int) -> bool: ...


class OutboxRepository(DraftRepository, Protocol):
    async def approve(
        self,
        draft_id: str,
        *,
        telegram_user_id: int,
        build_package: Callable[[EventCandidate, Draft], PublicationPackage],
    ) -> PublicationPackage | None:
        """In one transaction: re-read the event and latest draft, build the package,
        insert it with ON CONFLICT DO NOTHING, store feedback and mark the event APPROVED.
        Returns None and changes nothing when the state no longer allows approval."""
        ...

    async def get_package(self, event_id: str) -> PublicationPackage | None: ...

    async def list_packages(
        self, status: OutboxStatus, *, limit: int
    ) -> list[PublicationPackage]: ...

    async def count_packages(self, status: OutboxStatus) -> int: ...


class CostLedger(Protocol):
    async def reserve_cost(
        self, entry: CostEntry, *, since: datetime, limit_usd: Decimal
    ) -> tuple[int, Decimal] | None:
        """Atomically store the entry only if spend since `since` plus the entry stays within
        `limit_usd`. Returns (entry id, new total) or None when the limit would be exceeded."""
        ...

    async def settle_cost(self, entry_id: int, *, units: int, cost_usd: Decimal) -> None:
        """Lower a reservation to the actual cost. A reservation is never raised."""
        ...

    async def cost_since(self, since: datetime) -> Decimal: ...


class RunRepository(OutboxRepository, CostLedger, Protocol):
    async def start_run(self, run_id: str) -> None: ...

    async def finish_run(
        self,
        run_id: str,
        status: RunStatus,
        counters: PipelineCounters,
        error_code: str | None,
    ) -> None: ...

    async def fail_interrupted_runs(self) -> int: ...

    async def last_run(self, statuses: set[RunStatus] | None = None) -> PipelineRun | None: ...

    async def counters_since(self, since: datetime) -> PipelineCounters: ...

    async def source_health(self) -> list[SourceHealth]: ...

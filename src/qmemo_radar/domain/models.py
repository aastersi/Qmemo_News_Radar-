from datetime import datetime
from decimal import Decimal
from typing import Annotated
from uuid import uuid4

from pydantic import AfterValidator, BaseModel, ConfigDict, Field, HttpUrl, model_validator

from qmemo_radar.domain.enums import (
    DraftStatus,
    EventStatus,
    FactCheckStatus,
    OutboxStatus,
    RunStatus,
    SourceType,
)

QMEMO_URL_PLACEHOLDER = "{qmemo_url}"
X_POST_LIMIT = 280
X_LINK_LENGTH = 23  # every link counts as 23 characters on X


def _x_text_template(value: str) -> str:
    if value.count(QMEMO_URL_PLACEHOLDER) != 1:
        raise ValueError("x_text_template must contain {qmemo_url} exactly once")
    if len(value.replace(QMEMO_URL_PLACEHOLDER, "x" * X_LINK_LENGTH)) > X_POST_LIMIT:
        raise ValueError("x_text_template must fit 280 characters including the link")
    return value


XTextTemplate = Annotated[str, AfterValidator(_x_text_template)]


class DomainModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class Engagement(DomainModel):
    likes: int = Field(default=0, ge=0)
    reposts: int = Field(default=0, ge=0)
    replies: int = Field(default=0, ge=0)
    quotes: int = Field(default=0, ge=0)
    views: int | None = Field(default=None, ge=0)


class RawSourceItem(DomainModel):
    source: SourceType
    external_id: str = Field(min_length=1, max_length=255)
    url: HttpUrl
    author_id: str | None = None
    author_handle: str | None = None
    author_display_name: str | None = None
    original_text: str = Field(min_length=1)
    language: str | None = None
    published_at: datetime
    engagement: Engagement = Field(default_factory=Engagement)
    raw_payload: dict[str, object] = Field(default_factory=dict)
    source_key: str | None = Field(default=None, max_length=80)


class SourceFetch(DomainModel):
    """One configured source query. Its cursor is saved only after all items are stored.

    A failed fetch (error_code) may carry a cursor too: source state that must survive the
    failure, such as failed attempts. It is saved without counting as a success.
    """

    source_key: str = Field(min_length=1, max_length=80)
    items: tuple[RawSourceItem, ...] = ()
    cursor: str | None = None
    error_code: str | None = None
    # Source-specific diagnostic counters (e.g. files_checked), stored in pipeline_metrics as-is.
    stats: dict[str, int] = Field(default_factory=dict)


class EventCandidate(RawSourceItem):
    event_id: str = Field(default_factory=lambda: uuid4().hex)
    discovered_at: datetime
    normalized_text: str
    content_hash: str
    status: EventStatus = EventStatus.DISCOVERED
    filter_reason: str | None = None
    duplicate_of_event_id: str | None = None


class ScoreBreakdown(DomainModel):
    qmemo_relevance: int = Field(ge=0, le=30)
    quote_strength: int = Field(ge=0, le=20)
    discussion_potential: int = Field(ge=0, le=15)
    freshness: int = Field(ge=0, le=15)
    clarity: int = Field(ge=0, le=10)
    action_likelihood: int = Field(ge=0, le=10)
    risk_penalty: int = Field(ge=0, le=30)


class ScoreResult(DomainModel):
    event_id: str
    breakdown: ScoreBreakdown
    total: int = Field(ge=0, le=100)
    rationale: str = Field(min_length=1)
    recommended_format: str = Field(min_length=1)
    target_action: str = Field(min_length=1)
    fact_check_required: bool = False
    fact_check_note: str | None = None
    prompt_version: str = "deterministic-v1"
    model_name: str = "none"
    headline: str = Field(default="", max_length=160)
    summary: str = Field(default="", max_length=600)


class PublicationPackage(DomainModel):
    schema_version: int = 1
    package_id: str = Field(default_factory=lambda: uuid4().hex)
    event_id: str
    draft_id: str
    quote_text: str
    quote_author: str
    quote_language: str
    category_hint: str | None = None
    context_summary: str
    qmemo_text: str
    source_type: SourceType
    source_external_id: str
    source_url: HttpUrl
    source_published_at: datetime
    x_text_template: XTextTemplate
    x_text_short: str
    cta: str
    fact_check_status: FactCheckStatus
    fact_check_notes: tuple[str, ...] = ()
    approved_by_telegram_id: int
    approved_at: datetime
    idempotency_key: str
    status: OutboxStatus = OutboxStatus.APPROVED

    @model_validator(mode="after")
    def require_verified_facts(self) -> "PublicationPackage":
        if self.fact_check_status is not FactCheckStatus.VERIFIED:
            raise ValueError("A publication package requires fact_check_status=VERIFIED")
        return self


class PipelineCounters(BaseModel):
    model_config = ConfigDict(extra="forbid")

    collected: int = 0
    inserted: int = 0
    duplicates: int = 0
    filtered: int = 0
    scored: int = 0
    shortlisted: int = 0
    archived: int = 0
    source_errors: int = 0
    rank_failed: int = 0


class ScoredEvent(DomainModel):
    event: EventCandidate
    score: ScoreResult


class DraftText(DomainModel):
    """What a draft writer produces for one event. Checked against the source before use."""

    quote_text: str = Field(min_length=3, max_length=500)
    quote_speaker: str | None = Field(default=None, max_length=120)
    context_summary: str = Field(min_length=1, max_length=400)
    qmemo_text: str = Field(min_length=1, max_length=600)
    x_text_template: XTextTemplate
    x_text_short: str = Field(min_length=1, max_length=160)
    angle: str = Field(min_length=1, max_length=150)
    cta: str = Field(min_length=1, max_length=120)
    fact_check_required: bool
    fact_check_notes: tuple[Annotated[str, Field(max_length=200)], ...] = Field(
        default=(), max_length=3
    )
    prompt_version: str = "deterministic-v1"
    model_name: str = "none"


class Draft(DomainModel):
    """One immutable draft version; at most two versions exist per event."""

    draft_id: str = Field(default_factory=lambda: uuid4().hex)
    event_id: str
    version: int = Field(ge=1, le=2)
    quote_text: str = Field(min_length=3)
    quote_author: str = Field(min_length=1)
    quote_language: str = Field(min_length=1)
    context_summary: str
    qmemo_text: str
    x_text_template: XTextTemplate
    x_text_short: str
    angle: str
    cta: str
    fact_check_status: FactCheckStatus
    fact_check_notes: tuple[str, ...] = ()
    status: DraftStatus = DraftStatus.ACTIVE
    prompt_version: str
    model_name: str
    revision_instruction: str | None = None
    created_at: datetime


class PipelineRun(DomainModel):
    run_id: str
    status: RunStatus
    started_at: datetime
    finished_at: datetime | None = None
    counters: PipelineCounters
    error_code: str | None = None


class CostEntry(DomainModel):
    """One potentially paid external operation, written before the request is sent."""

    provider: str = Field(min_length=1, max_length=80)
    operation: str = Field(min_length=1, max_length=80)
    units: int = Field(ge=0)
    estimated_cost_usd: Decimal = Field(ge=0)
    created_at: datetime


class SourceHealth(DomainModel):
    source_key: str
    last_success_at: datetime | None = None
    last_error_at: datetime | None = None
    last_error: str | None = None
    consecutive_failures: int = 0
    # Parts of the source that failed repeatedly and are parked for an operator (GDELT minutes).
    blocked_gaps: int = 0

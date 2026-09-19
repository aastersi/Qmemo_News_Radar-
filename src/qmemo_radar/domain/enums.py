from enum import StrEnum


class SourceType(StrEnum):
    """Where an item came from. Stored as plain TEXT without a CHECK constraint, so a new
    member is a code change only: no database migration is needed."""

    X = "x"
    RSS = "rss"
    GDELT = "gdelt"
    BLUESKY = "bluesky"
    HACKER_NEWS = "hacker_news"
    GITHUB = "github"
    YOUTUBE = "youtube"
    MANUAL = "manual"


# Sources whose items share a URL (GDELT: one item per quote of an article). Their URL is not a
# duplicate key; the partial unique index in migration 009 must list the same sources.
SHARED_URL_SOURCES = frozenset({SourceType.GDELT})


class Metric(StrEnum):
    """Flow metrics stored per run and source in `pipeline_metrics`."""

    COLLECTED = "collected"  # items returned by a source
    INSERTED = "inserted"  # new rows, including filtered ones kept for retention
    EXACT_DUPLICATES = "exact_duplicates"  # already stored, or identical text to an earlier item
    FILTERED = "filtered"  # rejected by deterministic rules
    SOURCE_ERRORS = "source_errors"
    INVALID_ITEMS = "invalid_items"  # could not be normalized or stored (e.g. broken Unicode)
    # An exact copy of a stored text, kept as a mention of it instead of a row.
    MENTIONS_AGGREGATED = "mentions_aggregated"
    # Selection (M4). Gate rejections are recorded as rejected_<reason>.
    NEAR_DUPLICATES = "near_duplicates"
    CLUSTERS_CREATED = "clusters_created"
    CLUSTERS_MERGED = "clusters_merged"
    PRESELECTED = "preselected"
    LOCAL_LLM_CALLS = "local_llm_calls"
    TELEGRAM_DELIVERED = "telegram_delivered"


class EventStatus(StrEnum):
    DISCOVERED = "DISCOVERED"
    FILTERED_OUT = "FILTERED_OUT"
    SCORED = "SCORED"
    SHORTLISTED = "SHORTLISTED"
    NOTIFIED = "NOTIFIED"
    SNOOZED = "SNOOZED"
    SKIPPED = "SKIPPED"
    ARCHIVED = "ARCHIVED"
    DRAFTED = "DRAFTED"
    APPROVED = "APPROVED"
    EXPIRED = "EXPIRED"


class OutboxStatus(StrEnum):
    APPROVED = "APPROVED"
    QMEMO_PENDING = "QMEMO_PENDING"
    QMEMO_PUBLISHED = "QMEMO_PUBLISHED"
    X_PENDING = "X_PENDING"
    PUBLISHED = "PUBLISHED"
    FAILED = "FAILED"
    VERIFY_REQUIRED = "VERIFY_REQUIRED"


class FactCheckStatus(StrEnum):
    VERIFIED = "VERIFIED"
    NEEDS_REVIEW = "NEEDS_REVIEW"


class DeliveryKind(StrEnum):
    DIGEST = "digest"
    URGENT = "urgent"


class FeedbackAction(StrEnum):
    USE = "USE"
    SKIP = "SKIP"
    LATER = "LATER"
    REVISE_SHORTER = "REVISE_SHORTER"
    REVISE_ANGLE = "REVISE_ANGLE"
    REVISE_CUSTOM = "REVISE_CUSTOM"
    VERIFY = "VERIFY"
    ACCEPT = "ACCEPT"
    REJECT = "REJECT"


class RunStatus(StrEnum):
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    PARTIAL = "PARTIAL"
    FAILED = "FAILED"


class DraftStatus(StrEnum):
    ACTIVE = "ACTIVE"
    SUPERSEDED = "SUPERSEDED"
    ACCEPTED = "ACCEPTED"
    REJECTED = "REJECTED"

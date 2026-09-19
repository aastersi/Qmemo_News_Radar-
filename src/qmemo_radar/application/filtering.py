import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from qmemo_radar.domain import EventCandidate

MANUAL_SOURCE_KEY = "manual"
_NOISE = re.compile(r"https?://\S+|[@#$]\w+")


@dataclass(frozen=True, slots=True)
class FilterPolicy:
    max_age: timedelta
    minimum_text_length: int = 20
    blocked_authors: frozenset[str] = frozenset()
    blocked_terms: frozenset[str] = frozenset()


def first_filter_reason(
    event: EventCandidate,
    policy: FilterPolicy,
    *,
    now: datetime | None = None,
    check_age: bool = True,
) -> str | None:
    current = now or datetime.now(UTC)
    published_at = event.published_at
    if published_at.tzinfo is None:
        published_at = published_at.replace(tzinfo=UTC)

    # A manually submitted link is an explicit choice, so only automated sources are age-limited.
    automated = event.source_key != MANUAL_SOURCE_KEY
    if check_age and automated and published_at < current - policy.max_age:
        return "too_old"
    author_keys = {event.author_handle.casefold() if event.author_handle else None, event.author_id}
    if author_keys & policy.blocked_authors:
        return "blocked_author"
    if len(_NOISE.sub("", event.normalized_text).strip()) < policy.minimum_text_length:
        return "too_short"
    if any(term in event.normalized_text for term in policy.blocked_terms):
        return "blocked_term"
    return None

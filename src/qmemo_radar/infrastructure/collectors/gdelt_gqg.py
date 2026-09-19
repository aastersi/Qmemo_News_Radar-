"""GDELT Global Quotation Graph: free minute files of quotes extracted from world news.

https://blog.gdeltproject.org/announcing-the-global-quotation-graph/
File `YYYYMMDDHHMMSS.gqg.json.gz`: gzip JSONL, one article per line with `date`, `url`, `title`,
`lang` and `quotes[]` of `pre`/`quote`/`post`. Most minutes have no file (observed 2026-09:
one or two files per quarter hour, each online about a minute after its name), so 404 is normal.

Cursor `gdelt:gqg` = the next minute to check, `YYYYMMDDHHMMSS` UTC. Per checked minute:
- 200: parse; a readable file always moves the cursor past it. Malformed rows are skipped and
  counted; a corrupt or oversized file keeps the rows read before the problem and is counted.
  Retrying would read the same bytes, so skipping is the only way not to stall.
- 404: an expected gap, the cursor moves on. Minutes newer than now minus the safety lag are
  never requested, so a late file is not mistaken for a gap.
- network error, timeout, 429, 5xx (after retries) or any other status: the run stops there and
  the cursor stays on that minute, so the next run checks it again. Nothing is ever skipped.
"""

import asyncio
import gzip
import hashlib
import io
import json
import logging
import zlib
from collections import Counter
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import Any

import httpx
from pydantic import HttpUrl

from qmemo_radar.domain import RawSourceItem, SourceFetch, SourceType
from qmemo_radar.infrastructure.http import HttpFailure, Sleep, get_limited

logger = logging.getLogger(__name__)

SOURCE_KEY = "gdelt:gqg"
FILE_URL = "https://data.gdeltproject.org/gdeltv3/gqg/{minute:%Y%m%d%H%M%S}.gqg.json.gz"
_CURSOR_FORMAT = "%Y%m%d%H%M%S"
# Observed 2026-09: ~1.5 MB compressed, ~4 MB decompressed, ~2,700 articles, ~15,000 quotes.
MAX_COMPRESSED_BYTES = 50 * 1024 * 1024
MAX_DECOMPRESSED_BYTES = 256 * 1024 * 1024
MAX_LINE_BYTES = 4 * 1024 * 1024
MAX_ARTICLES_PER_FILE = 100_000
MAX_QUOTE_CHARS = 2_000
_CONTEXT_CHARS = 500
# Blocked minutes are retried every run; this bounds that work and turns a long outage into a
# visible stop (gdelt_blocked_gaps_full) instead of an ever-growing list.
MAX_BLOCKED_MINUTES = 30
MAX_SKIPPED_RECORDS = 200


class GdeltQuotationCollector:
    def __init__(
        self,
        http: httpx.AsyncClient,
        *,
        safety_lag: timedelta,
        max_minutes_per_run: int,
        first_run_lookback: timedelta,
        languages: frozenset[str] | None,
        allow_unknown_language: bool,
        block_after_failures: int = 3,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        sleep: Sleep = asyncio.sleep,
    ) -> None:
        self._http = http
        self._safety_lag = safety_lag
        self._max_minutes = max_minutes_per_run
        self._lookback = first_run_lookback
        self._languages = languages
        self._allow_unknown = allow_unknown_language
        self._block_after = block_after_failures
        self._clock = clock
        self._sleep = sleep

    async def collect(self, checkpoints: Mapping[str, str]) -> list[SourceFetch]:
        now = self._clock()
        newest = _floor_minute(now - self._safety_lag)
        state = GdeltCursor.parse(checkpoints.get(SOURCE_KEY))
        start = state.next or _floor_minute(now - self._lookback)
        state.next = start
        stats: Counter[str] = Counter()
        fetches: list[SourceFetch] = []
        if start > _floor_minute(now):
            # The clock went back or the cursor is corrupt: idling silently would look healthy.
            return self._stop(fetches, state, start, start, stats, "gdelt_cursor_ahead")
        await self._retry_blocked(state, fetches, stats)

        minute = start
        while minute <= newest and stats["files_checked"] < self._max_minutes:
            stats["files_checked"] += 1
            outcome = await self._check(minute, stats)
            if isinstance(outcome, str):
                if outcome == "gdelt_clock_ahead":
                    # Our clock runs ahead of the GDELT clock: the file may still appear, no gap.
                    return self._stop(fetches, state, start, minute, stats, outcome)
                attempts = state.attempts + 1 if minute == start else 1
                if attempts < self._block_after or len(state.blocked) >= MAX_BLOCKED_MINUTES:
                    state.attempts = attempts
                    code = outcome if attempts < self._block_after else "gdelt_blocked_gaps_full"
                    return self._stop(fetches, state, start, minute, stats, code)
                # Failed in `attempts` runs in a row: park it, visibly, and keep collecting.
                state.blocked[_cursor(minute)] = {
                    "reason": outcome,
                    "attempts": attempts,
                    "since": now.isoformat(),
                }
                stats["gaps_blocked"] += 1
                _warn("gdelt minute blocked", minute, outcome)
            elif outcome is not None:
                fetches.append(self._fetch(state, minute + _MINUTE, outcome))
            minute += _MINUTE
        state.next, state.attempts = minute, 0
        fetches.append(SourceFetch(source_key=SOURCE_KEY, cursor=state.dump(), stats=stats))
        return fetches

    async def _retry_blocked(
        self, state: "GdeltCursor", fetches: list[SourceFetch], stats: Counter[str]
    ) -> None:
        """One attempt per blocked minute and run; the first failure ends it (likely an outage)."""
        for name in sorted(state.blocked):
            minute = _parse_minute(name)
            outcome = await self._check(minute, stats, attempts=1) if minute else None
            if isinstance(outcome, str):
                state.blocked[name]["attempts"] = int(state.blocked[name].get("attempts", 0)) + 1
                return
            del state.blocked[name]
            stats["gaps_recovered"] += 1
            # Saved with its items: the minute leaves the list only once they are stored.
            fetches.append(
                SourceFetch(source_key=SOURCE_KEY, items=outcome or (), cursor=state.dump())
            )

    def _fetch(
        self, state: "GdeltCursor", next_minute: datetime | None, items: tuple[RawSourceItem, ...]
    ) -> SourceFetch:
        state.next, state.attempts = next_minute, 0
        return SourceFetch(source_key=SOURCE_KEY, items=items, cursor=state.dump())

    async def _check(
        self, minute: datetime, stats: Counter[str], *, attempts: int = 3
    ) -> tuple[RawSourceItem, ...] | str | None:
        """Items of a found file, None for a gap, or the error code of a failed minute."""
        try:
            response, body = await get_limited(
                self._http,
                FILE_URL.format(minute=minute),
                max_bytes=MAX_COMPRESSED_BYTES,
                attempts=attempts,
                sleep=self._sleep,
            )
        except HttpFailure as exc:
            if exc.code != "response_too_large":
                return f"gdelt_{exc.code}"
            stats["files_oversized"] += 1
            _warn("gdelt file too large", minute, exc.code)
            return None
        if response.status_code == 404:
            if self._too_new_for_server(minute, response):
                return "gdelt_clock_ahead"
            stats["expected_gaps"] += 1
            return None
        if response.status_code != 200:
            return f"gdelt_http_{response.status_code}"
        stats["files_found"] += 1
        items, file_stats = await asyncio.to_thread(self._parse, body, minute)
        stats.update(file_stats)
        return items

    def _stop(
        self,
        fetches: list[SourceFetch],
        state: "GdeltCursor",
        start: datetime,
        minute: datetime,
        stats: Counter[str],
        code: str,
    ) -> list[SourceFetch]:
        """Save the progress before `minute` (gaps included), report the error, retry it next run.

        Without progress no success is recorded, so a minute that keeps failing shows a growing
        consecutive_failures in /status instead of resetting it every run. The error still
        carries the cursor: failed attempts and blocked minutes must survive the run.
        """
        _warn("gdelt file failed", minute, code)
        attempts = state.attempts
        moved = [self._fetch(state, minute, ())] if minute > start else []
        state.next, state.attempts = minute, attempts
        error = SourceFetch(
            source_key=SOURCE_KEY, error_code=code, stats=stats, cursor=state.dump()
        )
        return [*fetches, *moved, error]

    def _too_new_for_server(self, minute: datetime, response: httpx.Response) -> bool:
        try:
            server_now = parsedate_to_datetime(response.headers.get("date", ""))
        except (TypeError, ValueError):
            return False  # no usable Date header: trust the local clock
        # One minute of tolerance for ordinary clock skew; the safety lag covers the rest.
        return minute > server_now - self._safety_lag + _MINUTE

    def _parse(
        self, body: bytes, minute: datetime
    ) -> tuple[tuple[RawSourceItem, ...], Counter[str]]:
        """Stream-decompress one file line by line; never holds the decompressed file in memory."""
        stats: Counter[str] = Counter()
        items: list[RawSourceItem] = []
        name = _cursor(minute)
        total = rows = 0
        try:
            with gzip.GzipFile(fileobj=io.BytesIO(body)) as stream:
                while line := stream.readline(MAX_LINE_BYTES + 1):
                    total += len(line)
                    oversized = len(line) > MAX_LINE_BYTES
                    while line and not line.endswith(b"\n") and oversized:  # skip the whole row
                        if total > MAX_DECOMPRESSED_BYTES:
                            break
                        line = stream.readline(MAX_LINE_BYTES)
                        total += len(line)
                    rows += 1
                    if total > MAX_DECOMPRESSED_BYTES or rows > MAX_ARTICLES_PER_FILE:
                        stats["files_truncated"] += 1
                        _warn("gdelt file truncated", minute, "limit_reached")
                        break
                    if oversized:
                        stats["malformed_rows"] += 1
                    elif line.strip():
                        try:
                            items.extend(self._article(line, name, stats))
                        except Exception:  # any row the checks below missed: skip, never stall
                            stats["malformed_rows"] += 1
        except (OSError, EOFError, zlib.error) as exc:  # gzip.BadGzipFile is an OSError
            stats["files_corrupt"] += 1
            _warn("gdelt file corrupt", minute, type(exc).__name__)
        return tuple(items), stats

    def _article(self, line: bytes, name: str, stats: Counter[str]) -> list[RawSourceItem]:
        try:
            record = json.loads(line)
        except ValueError:  # includes UnicodeDecodeError
            stats["malformed_rows"] += 1
            return []
        if not isinstance(record, dict):
            stats["malformed_rows"] += 1
            return []
        stats["articles_seen"] += 1
        quotes = record.get("quotes")
        lang = record.get("lang")
        if isinstance(quotes, list):
            stats["quotes_seen"] += len(quotes)
        # Language first: everything below is skipped for articles nobody reads.
        if not isinstance(lang, str) or not lang.strip():
            if not self._allow_unknown:
                stats["articles_language_skipped"] += 1
                return []
            lang = None
        elif self._languages is not None and lang.strip().casefold() not in self._languages:
            stats["articles_language_skipped"] += 1
            return []
        url, date = record.get("url"), record.get("date")
        try:
            published_at = datetime.fromisoformat(date) if isinstance(date, str) else None
        except ValueError:
            published_at = None
        if not isinstance(url, str) or not isinstance(quotes, list) or published_at is None:
            stats["malformed_rows"] += 1
            return []
        if published_at.tzinfo is None:
            published_at = published_at.replace(tzinfo=UTC)

        items: list[RawSourceItem] = []
        for quote in quotes:
            item = self._quote(record, quote, url, lang, published_at, name, stats)
            if item is not None:
                items.append(item)
        stats["quotes_accepted"] += len(items)
        return items

    def _quote(
        self,
        record: dict[str, Any],
        quote: object,
        url: str,
        lang: str | None,
        published_at: datetime,
        name: str,
        stats: Counter[str],
    ) -> RawSourceItem | None:
        text = quote.get("quote") if isinstance(quote, dict) else None
        if not isinstance(text, str) or not text.strip():
            stats["quotes_rejected"] += 1
            return None
        text = text.strip()
        if len(text) > MAX_QUOTE_CHARS:
            stats["quotes_too_long"] += 1
            return None
        assert isinstance(quote, dict)
        try:
            return RawSourceItem(
                source=SourceType.GDELT,
                external_id=hashlib.sha256(f"{url}\n{text}".encode()).hexdigest()[:32],
                url=HttpUrl(url),
                # GDELT gives no structured speaker; pre/post context stays in raw_payload.
                author_display_name=None,
                original_text=text,
                language=lang,
                # When GDELT processed the article, not when the outlet published it.
                published_at=published_at,
                raw_payload={
                    "title": _context(record.get("title")),
                    "pre": _context(quote.get("pre")),
                    "quote": text,
                    "post": _context(quote.get("post")),
                    "url": url,
                    "lang": lang,
                    "date": record.get("date"),
                    "gqg_file": name,
                },
                source_key=SOURCE_KEY,
            )
        except ValueError:  # pydantic ValidationError, or text with lone surrogates
            stats["quotes_rejected"] += 1
            return None


_MINUTE = timedelta(minutes=1)


def _floor_minute(moment: datetime) -> datetime:
    return moment.astimezone(UTC).replace(second=0, microsecond=0)


def _cursor(minute: datetime) -> str:
    return minute.strftime(_CURSOR_FORMAT)


def _parse_minute(value: str) -> datetime | None:
    try:
        return _floor_minute(datetime.strptime(value, _CURSOR_FORMAT).replace(tzinfo=UTC))
    except ValueError:
        return None


@dataclass
class GdeltCursor:
    """`YYYYMMDDHHMMSS` while nothing failed; JSON once attempts or blocked minutes exist.

    blocked: minutes that failed in `block_after_failures` runs in a row. Each is retried once per
    run until it loads, or until an operator skips it (`qmemo-radar gaps --skip`); `skipped` keeps
    that record. Nothing leaves `blocked` silently.
    """

    next: datetime | None = None
    attempts: int = 0
    blocked: dict[str, dict[str, Any]] = field(default_factory=dict)
    skipped: dict[str, dict[str, Any]] = field(default_factory=dict)

    @classmethod
    def parse(cls, value: str | None) -> "GdeltCursor":
        if not value:
            return cls()
        try:
            if not value.startswith("{"):
                parsed = _parse_minute(value)
                if parsed is None:
                    raise ValueError(value)
                return cls(next=parsed)
            data = json.loads(value)
            return cls(
                next=_parse_minute(str(data["next"])) if data.get("next") else None,
                attempts=int(data.get("attempts", 0)),
                blocked=dict(data.get("blocked") or {}),
                skipped=dict(data.get("skipped") or {}),
            )
        except (ValueError, TypeError, KeyError, AttributeError):
            logger.warning(
                "gdelt cursor invalid, starting from the lookback window",
                extra={
                    "operation": "collect",
                    "source_key": SOURCE_KEY,
                    "error_code": "bad_cursor",
                },
            )
            return cls()

    def dump(self) -> str | None:
        next_minute = _cursor(self.next) if self.next else None
        if not (self.attempts or self.blocked or self.skipped):
            return next_minute
        # The newest skip records only: an audit trail, not a growing table.
        skipped = dict(sorted(self.skipped.items())[-MAX_SKIPPED_RECORDS:])
        data = {
            "next": next_minute,
            "attempts": self.attempts,
            "blocked": self.blocked,
            "skipped": skipped,
        }
        return json.dumps(data, sort_keys=True)

    def skip(self, minutes: Iterable[str], *, at: datetime) -> list[str]:
        """Give up on blocked minutes; returns the ones that were blocked."""
        done = []
        for name in minutes:
            entry = self.blocked.pop(name, None)
            if entry is not None:
                self.skipped[name] = {**entry, "skipped_at": at.isoformat()}
                done.append(name)
        return done


def _context(value: object) -> str | None:
    return value[:_CONTEXT_CHARS] if isinstance(value, str) else None


def _warn(message: str, minute: datetime, code: str) -> None:
    logger.warning(
        message,
        extra={
            "operation": "collect",
            "source_key": SOURCE_KEY,
            "result": _cursor(minute),
            "error_code": code,
        },
    )

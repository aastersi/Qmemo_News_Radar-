import json
import sqlite3
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from decimal import ROUND_CEILING, Decimal
from importlib.resources import files
from pathlib import Path

import aiosqlite

from qmemo_radar.application.filtering import ITEM_REASONS
from qmemo_radar.application.normalization import comparison_text
from qmemo_radar.application.ports import (
    ClusterJoin,
    ClusterScore,
    KnownEvents,
    Mention,
    PruneCutoffs,
    RejectedSample,
    Representative,
)
from qmemo_radar.application.selection import ClusterSignals, article_of, domain_of
from qmemo_radar.domain import (
    SHARED_URL_SOURCES,
    CostEntry,
    DeliveryKind,
    Draft,
    DraftStatus,
    Engagement,
    EventCandidate,
    EventStatus,
    FactCheckStatus,
    FeedbackAction,
    OutboxStatus,
    PipelineCounters,
    PipelineRun,
    PublicationPackage,
    RunStatus,
    ScoreBreakdown,
    ScoredEvent,
    ScoreResult,
    SourceHealth,
)

_MIGRATIONS = "qmemo_radar.infrastructure.storage.migrations"
_APPLIED_VERSIONS = "SELECT version FROM schema_migrations"
_SCORED_EVENTS = """
    SELECT e.*, s.* FROM radar_events e
    JOIN event_scores s ON s.event_id = e.id
"""
_INSERT_EVENT = """
    INSERT OR IGNORE INTO radar_events (
        id, source, external_id, url, author_id, author_handle,
        author_display_name, original_text, normalized_text,
        content_hash, language, published_at, discovered_at,
        engagement_json, raw_payload_json, status, created_at, updated_at,
        source_key, filter_reason, domain, article, duplicate_of_event_id
    ) VALUES (
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
        -- NULL instead of a foreign-key error when the original was ignored as a duplicate
        -- (e.g. a manual link stored it between find_known and this insert).
        (SELECT id FROM radar_events WHERE id = ?)
    )
"""
# Retention candidates; everything a person touched is kept regardless of status.
_NOISE = (EventStatus.FILTERED_OUT, EventStatus.EXPIRED, EventStatus.ARCHIVED)
_EXPIRABLE = (
    EventStatus.DISCOVERED,
    EventStatus.SCORED,
    EventStatus.SHORTLISTED,
    EventStatus.NOTIFIED,
    EventStatus.SNOOZED,
    EventStatus.DRAFTED,
)


class SQLiteEventRepository:
    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path

    async def initialize(self) -> None:
        """Apply every packaged migration that is not recorded yet, in file-name order."""
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        migrations = sorted(
            (item for item in files(_MIGRATIONS).iterdir() if item.name.endswith(".sql")),
            key=lambda item: item.name,
        )
        async with self._connect() as db:
            await db.execute(
                "CREATE TABLE IF NOT EXISTS schema_migrations "
                "(version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL)"
            )
            applied = {row[0] for row in await db.execute_fetchall(_APPLIED_VERSIONS)}
            for migration in migrations:
                if int(migration.name.split("_", 1)[0]) not in applied:
                    await db.executescript(migration.read_text(encoding="utf-8"))
            await db.commit()

    async def add_event(self, event: EventCandidate) -> bool:
        return await self.add_events([event]) == 1

    async def add_events(
        self, events: Sequence[EventCandidate], mentions: Sequence[Mention] = ()
    ) -> int:
        if not events and not mentions:
            return 0
        now = datetime.now(UTC).isoformat()
        async with self._transaction() as db:
            before = db.total_changes
            await db.executemany(_INSERT_EVENT, [_event_row(event, now) for event in events])
            inserted = db.total_changes - before
            if not mentions:
                return inserted
            await db.executemany(
                """
                INSERT OR IGNORE INTO content_mentions (
                    event_id, url, domain, article, source_key, seen_at
                )
                SELECT ?, ?, ?, ?, ?, ? WHERE EXISTS (SELECT 1 FROM radar_events WHERE id = ?)
                """,
                [
                    (
                        m.event_id,
                        m.url,
                        m.domain,
                        m.article,
                        m.source_key,
                        _iso(m.seen_at),
                        m.event_id,
                    )
                    for m in mentions
                ],
            )
            owners = sorted({mention.event_id for mention in mentions})
            for chunk in _chunks(owners):
                await db.execute(
                    f"""
                    UPDATE event_clusters SET preselect_score = -1
                    WHERE id IN (
                        SELECT cluster_id FROM radar_events
                        WHERE id IN ({_placeholders(chunk)}) AND cluster_id IS NOT NULL
                    )
                    """,
                    chunk,
                )
            return inserted

    async def find_known(self, events: Sequence[EventCandidate]) -> KnownEvents:
        by_source: dict[str, list[EventCandidate]] = {}
        for event in events:
            by_source.setdefault(event.source.value, []).append(event)
        ids: set[tuple[str, str]] = set()
        urls: set[tuple[str, str]] = set()
        owners: dict[str, str] = {}
        async with self._connect() as db:
            for source, group in by_source.items():
                external_ids = [event.external_id for event in group]
                rows = await db.execute_fetchall(
                    f"SELECT external_id FROM radar_events WHERE source = ? "
                    f"AND external_id IN ({_placeholders(external_ids)})",
                    (source, *external_ids),
                )
                ids.update((source, str(row[0])) for row in rows)
                if source in SHARED_URL_SOURCES:
                    continue  # not a duplicate key, and the URL index does not cover this source
                group_urls = [str(event.url) for event in group]
                rows = await db.execute_fetchall(
                    # `source != 'gdelt'` lets SQLite use the partial index idx_events_source_url.
                    f"SELECT url FROM radar_events WHERE source = ? AND source != 'gdelt' "
                    f"AND url IN ({_placeholders(group_urls)})",
                    (source, *group_urls),
                )
                urls.update((source, str(row[0])) for row in rows)
            hashes = sorted({event.content_hash for event in events})
            if hashes:
                rows = await db.execute_fetchall(
                    f"""
                    SELECT content_hash, id, MIN(rowid) FROM radar_events
                    WHERE content_hash IN ({_placeholders(hashes)})
                      AND COALESCE(filter_reason, '') NOT IN ({_placeholders(_NOT_OWNERS)})
                    GROUP BY content_hash
                    """,
                    [*hashes, *_NOT_OWNERS],
                )
                owners = {str(row[0]): str(row[1]) for row in rows}
            pairs = sorted(
                {
                    (owners[event.content_hash], str(event.url))
                    for event in events
                    if event.content_hash in owners
                }
            )
            mentioned: set[tuple[str, str]] = set()
            for chunk in _chunks(pairs):
                found = await db.execute_fetchall(
                    f"SELECT event_id, url FROM content_mentions WHERE (event_id, url) IN "
                    f"(VALUES {','.join('(?, ?)' for _ in chunk)})",
                    [value for pair in chunk for value in pair],
                )
                mentioned.update((str(row[0]), str(row[1])) for row in found)
        return KnownEvents(
            ids=frozenset(ids),
            urls=frozenset(urls),
            content_owners=owners,
            mentions=frozenset(mentioned),
        )

    async def record_metrics(self, run_id: str, metrics: Mapping[str, Mapping[str, int]]) -> None:
        now = datetime.now(UTC).isoformat()
        rows = [
            (run_id, source_key, str(metric), value, now)
            for source_key, values in metrics.items()
            for metric, value in values.items()
            if value
        ]
        if not rows:
            return
        async with self._transaction() as db:
            await db.executemany(
                """
                INSERT INTO pipeline_metrics (run_id, source_key, metric, value, recorded_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(run_id, source_key, metric) DO UPDATE SET
                    value = value + excluded.value
                """,
                rows,
            )

    async def metrics_since(self, since: datetime) -> dict[str, dict[str, int]]:
        async with self._connect() as db:
            rows = await db.execute_fetchall(
                """
                SELECT source_key, metric, SUM(value) FROM pipeline_metrics
                WHERE recorded_at >= ? GROUP BY source_key, metric ORDER BY source_key, metric
                """,
                (since.astimezone(UTC).isoformat(),),
            )
        result: dict[str, dict[str, int]] = {}
        for source_key, metric, value in rows:
            result.setdefault(str(source_key), {})[str(metric)] = int(value)
        return result

    async def set_status(
        self,
        event_id: str,
        status: EventStatus,
        *,
        expected: set[EventStatus] | None = None,
        filter_reason: str | None = None,
    ) -> bool:
        parameters: list[object] = [
            status.value,
            filter_reason,
            datetime.now(UTC).isoformat(),
            event_id,
        ]
        query = """
            UPDATE radar_events
            SET status = ?, filter_reason = COALESCE(?, filter_reason), updated_at = ?
            WHERE id = ?
        """
        if expected:
            values = sorted(item.value for item in expected)
            placeholders = ",".join("?" for _ in values)
            query += f" AND status IN ({placeholders})"
            parameters.extend(values)

        async with self._connect() as db:
            cursor = await db.execute(query, parameters)
            await db.commit()
            return cursor.rowcount == 1

    async def list_events_by_status(
        self,
        status: EventStatus,
        *,
        limit: int,
    ) -> list[EventCandidate]:
        async with self._connect() as db:
            cursor = await db.execute(
                """
                SELECT * FROM radar_events
                WHERE status = ?
                -- Newest first: a large free-source backlog must not starve fresh candidates;
                -- older ones expire by TTL.
                ORDER BY discovered_at DESC
                LIMIT ?
                """,
                (status.value, limit),
            )
            rows = await cursor.fetchall()
        return [self._event_from_row(row) for row in rows]

    async def save_score_and_status(
        self,
        score: ScoreResult,
        status: EventStatus,
    ) -> bool:
        now = datetime.now(UTC).isoformat()
        breakdown = score.breakdown
        async with self._connect() as db:
            await db.execute("BEGIN IMMEDIATE")
            await db.execute(
                """
                INSERT INTO event_scores (
                    event_id, qmemo_relevance, quote_strength, discussion_potential,
                    freshness, clarity, action_likelihood, risk_penalty, total,
                    rationale, recommended_format, target_action,
                    fact_check_required, fact_check_note, prompt_version,
                    model_name, scored_at, headline, summary
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(event_id) DO UPDATE SET
                    qmemo_relevance = excluded.qmemo_relevance,
                    quote_strength = excluded.quote_strength,
                    discussion_potential = excluded.discussion_potential,
                    freshness = excluded.freshness,
                    clarity = excluded.clarity,
                    action_likelihood = excluded.action_likelihood,
                    risk_penalty = excluded.risk_penalty,
                    total = excluded.total,
                    rationale = excluded.rationale,
                    recommended_format = excluded.recommended_format,
                    target_action = excluded.target_action,
                    fact_check_required = excluded.fact_check_required,
                    fact_check_note = excluded.fact_check_note,
                    prompt_version = excluded.prompt_version,
                    model_name = excluded.model_name,
                    scored_at = excluded.scored_at,
                    headline = excluded.headline,
                    summary = excluded.summary
                """,
                (
                    score.event_id,
                    breakdown.qmemo_relevance,
                    breakdown.quote_strength,
                    breakdown.discussion_potential,
                    breakdown.freshness,
                    breakdown.clarity,
                    breakdown.action_likelihood,
                    breakdown.risk_penalty,
                    score.total,
                    score.rationale,
                    score.recommended_format,
                    score.target_action,
                    int(score.fact_check_required),
                    score.fact_check_note,
                    score.prompt_version,
                    score.model_name,
                    now,
                    score.headline,
                    score.summary,
                ),
            )
            cursor = await db.execute(
                """
                UPDATE radar_events
                SET status = ?, updated_at = ?
                WHERE id = ? AND status = ?
                """,
                (
                    status.value,
                    now,
                    score.event_id,
                    EventStatus.DISCOVERED.value,
                ),
            )
            if cursor.rowcount != 1:
                await db.rollback()
                return False
            await db.commit()
            return True

    async def count_by_status(self) -> dict[str, int]:
        async with self._connect() as db:
            cursor = await db.execute(
                "SELECT status, COUNT(*) AS amount FROM radar_events GROUP BY status"
            )
            rows = await cursor.fetchall()
            return {str(row[0]): int(row[1]) for row in rows}

    async def get_checkpoints(self) -> dict[str, str]:
        async with self._connect() as db:
            rows = await db.execute_fetchall(
                "SELECT source_key, cursor_value FROM source_checkpoints "
                "WHERE cursor_value IS NOT NULL"
            )
        return {str(row[0]): str(row[1]) for row in rows}

    async def record_source_result(
        self,
        source_key: str,
        *,
        cursor: str | None,
        error_code: str | None,
    ) -> None:
        now = datetime.now(UTC).isoformat()
        async with self._connect() as db:
            if error_code is None:
                await db.execute(
                    """
                    INSERT INTO source_checkpoints (source_key, cursor_value, last_success_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(source_key) DO UPDATE SET
                        cursor_value = COALESCE(excluded.cursor_value, cursor_value),
                        last_success_at = excluded.last_success_at,
                        consecutive_failures = 0
                    """,
                    (source_key, cursor, now),
                )
            else:
                await db.execute(
                    """
                    INSERT INTO source_checkpoints (
                        source_key, cursor_value, last_error_at, last_error, consecutive_failures
                    ) VALUES (?, ?, ?, ?, 1)
                    ON CONFLICT(source_key) DO UPDATE SET
                        cursor_value = COALESCE(excluded.cursor_value, cursor_value),
                        last_error_at = excluded.last_error_at,
                        last_error = excluded.last_error,
                        consecutive_failures = consecutive_failures + 1
                    """,
                    (source_key, cursor, now, error_code),
                )
            await db.commit()

    async def has_earlier_content_duplicate(self, event: EventCandidate) -> bool:
        async with self._connect() as db:
            # The same owner rule as find_known: an earlier row filtered for its item is no owner.
            rows = await db.execute_fetchall(
                f"""
                SELECT 1 FROM radar_events
                WHERE content_hash = ?
                  AND rowid < (SELECT rowid FROM radar_events WHERE id = ?)
                  AND COALESCE(filter_reason, '') NOT IN ({_placeholders(_NOT_OWNERS)})
                LIMIT 1
                """,
                (event.content_hash, event.event_id, *_NOT_OWNERS),
            )
        return bool(rows)

    async def list_deliverable(
        self,
        statuses: set[EventStatus],
        *,
        min_total: int,
        limit: int,
    ) -> list[ScoredEvent]:
        values = sorted(status.value for status in statuses)
        async with self._connect() as db:
            rows = await db.execute_fetchall(
                f"""{_SCORED_EVENTS}
                WHERE e.status IN ({_placeholders(values)})
                  AND (s.total >= ? OR e.source_key = 'manual')
                ORDER BY s.total DESC, e.published_at DESC
                LIMIT ?
                """,
                (*values, min_total, limit),
            )
        return [self._scored_from_row(row) for row in rows]

    async def record_delivery(
        self,
        event_id: str,
        *,
        chat_id: int,
        message_id: int,
        kind: DeliveryKind,
        expected: set[EventStatus],
    ) -> bool:
        values = sorted(status.value for status in expected)
        now = datetime.now(UTC).isoformat()
        async with self._transaction() as db:
            [(previous,)] = await db.execute_fetchall(
                "SELECT COUNT(*) FROM telegram_deliveries WHERE event_id = ?", (event_id,)
            )
            await db.execute(
                """
                INSERT INTO telegram_deliveries (
                    event_id, chat_id, message_id, delivery_type, delivered_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (event_id, chat_id, message_id, f"{kind.value}:{previous + 1}", now),
            )
            cursor = await db.execute(
                f"""
                UPDATE radar_events SET status = ?, updated_at = ?
                WHERE id = ? AND status IN ({_placeholders(values)})
                """,
                (EventStatus.NOTIFIED.value, now, event_id, *values),
            )
            if cursor.rowcount != 1:
                await db.rollback()
                return False
        return True

    async def count_deliveries_since(self, since: datetime) -> int:
        async with self._connect() as db:
            [(count,)] = await db.execute_fetchall(
                "SELECT COUNT(*) FROM telegram_deliveries WHERE delivered_at >= ?",
                (since.astimezone(UTC).isoformat(),),
            )
        return int(count)

    async def list_delivered_since(self, since: datetime) -> list[ScoredEvent]:
        async with self._connect() as db:
            rows = await db.execute_fetchall(
                f"""{_SCORED_EVENTS}
                WHERE e.id IN (
                    SELECT event_id FROM telegram_deliveries WHERE delivered_at >= ?
                )
                ORDER BY s.total DESC
                """,
                (since.astimezone(UTC).isoformat(),),
            )
        return [self._scored_from_row(row) for row in rows]

    async def list_scored_by_status(self, status: EventStatus, *, limit: int) -> list[ScoredEvent]:
        async with self._connect() as db:
            rows = await db.execute_fetchall(
                f"{_SCORED_EVENTS} WHERE e.status = ? ORDER BY e.updated_at DESC LIMIT ?",
                (status.value, limit),
            )
        return [self._scored_from_row(row) for row in rows]

    async def get_scored_event(self, event_id: str) -> ScoredEvent | None:
        async with self._connect() as db:
            rows = list(await db.execute_fetchall(f"{_SCORED_EVENTS} WHERE e.id = ?", (event_id,)))
        return self._scored_from_row(rows[0]) if rows else None

    async def decide(
        self,
        event_id: str,
        status: EventStatus,
        *,
        expected: set[EventStatus],
        action: FeedbackAction,
        telegram_user_id: int,
    ) -> bool:
        values = sorted(item.value for item in expected)
        now = datetime.now(UTC).isoformat()
        async with self._transaction() as db:
            cursor = await db.execute(
                f"""
                UPDATE radar_events SET status = ?, updated_at = ?
                WHERE id = ? AND status IN ({_placeholders(values)})
                """,
                (status.value, now, event_id, *values),
            )
            if cursor.rowcount != 1:
                await db.rollback()
                return False
            await _insert_feedback(db, event_id, None, action, telegram_user_id, now)
        return True

    async def expire_events(self, discovered_before: datetime) -> int:
        values = [status.value for status in _EXPIRABLE]
        async with self._connect() as db:
            cursor = await db.execute(
                f"""
                UPDATE radar_events SET status = ?, updated_at = ?
                WHERE status IN ({_placeholders(values)}) AND discovered_at < ?
                """,
                (
                    EventStatus.EXPIRED.value,
                    datetime.now(UTC).isoformat(),
                    *values,
                    discovered_before.astimezone(UTC).isoformat(),
                ),
            )
            await db.commit()
            return cursor.rowcount

    async def requeue_manual(self, event: EventCandidate) -> bool:
        async with self._connect() as db:
            cursor = await db.execute(
                """
                UPDATE radar_events
                SET source_key = 'manual', filter_reason = NULL, updated_at = ?,
                    status = CASE WHEN status = ? THEN ? ELSE ? END
                WHERE source = ? AND external_id = ? AND status IN (?, ?)
                """,
                (
                    datetime.now(UTC).isoformat(),
                    EventStatus.ARCHIVED.value,
                    EventStatus.SHORTLISTED.value,
                    EventStatus.DISCOVERED.value,
                    event.source.value,
                    event.external_id,
                    EventStatus.ARCHIVED.value,
                    EventStatus.FILTERED_OUT.value,
                ),
            )
            await db.commit()
            return cursor.rowcount == 1

    async def sample_events(
        self, source: str, *, limit: int, random: bool
    ) -> list[dict[str, object]]:
        """Newest (or random) stored items of a source (`gdelt`) or source key (`rss:wire`).

        Opens the file read-only: this can neither migrate nor change the database.
        """
        column = "source_key" if ":" in source else "source"
        order = "RANDOM()" if random else "rowid DESC"
        async with aiosqlite.connect(f"{self._db_path.resolve().as_uri()}?mode=ro", uri=True) as db:
            db.row_factory = aiosqlite.Row
            rows = await db.execute_fetchall(
                f"""
                SELECT source, source_key, status, filter_reason, published_at, discovered_at,
                       author_handle, author_display_name, language, original_text, url,
                       json_extract(raw_payload_json, '$.title') AS title
                FROM radar_events WHERE {column} = ? ORDER BY {order} LIMIT ?
                """,
                (source, limit),
            )
        return [dict(row) for row in rows]

    async def get_state(self, key: str) -> str | None:
        async with self._connect() as db:
            rows = list(
                await db.execute_fetchall("SELECT value FROM radar_state WHERE key = ?", (key,))
            )
        return str(rows[0][0]) if rows else None

    async def set_state(self, key: str, value: str) -> None:
        async with self._connect() as db:
            await db.execute(
                """
                INSERT INTO radar_state (key, value, updated_at) VALUES (?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value = excluded.value,
                    updated_at = excluded.updated_at
                """,
                (key, value, datetime.now(UTC).isoformat()),
            )
            await db.commit()

    async def get_draft(self, draft_id: str) -> Draft | None:
        async with self._connect() as db:
            rows = list(await db.execute_fetchall("SELECT * FROM drafts WHERE id = ?", (draft_id,)))
        return _draft_from_row(rows[0]) if rows else None

    async def latest_draft(self, event_id: str) -> Draft | None:
        async with self._connect() as db:
            rows = list(
                await db.execute_fetchall(
                    "SELECT * FROM drafts WHERE event_id = ? ORDER BY version DESC LIMIT 1",
                    (event_id,),
                )
            )
        return _draft_from_row(rows[0]) if rows else None

    async def latest_revisable_draft(self) -> Draft | None:
        async with self._connect() as db:
            rows = list(
                await db.execute_fetchall(
                    """
                    SELECT d.* FROM drafts d JOIN radar_events e ON e.id = d.event_id
                    WHERE d.status = ? AND d.version = 1 AND e.status = ?
                    ORDER BY d.created_at DESC LIMIT 1
                    """,
                    (DraftStatus.ACTIVE.value, EventStatus.DRAFTED.value),
                )
            )
        return _draft_from_row(rows[0]) if rows else None

    async def save_first_draft(self, draft: Draft, *, telegram_user_id: int) -> bool:
        now = datetime.now(UTC).isoformat()
        try:
            async with self._transaction() as db:
                cursor = await db.execute(
                    """
                    UPDATE radar_events SET status = ?, updated_at = ?
                    WHERE id = ? AND status IN (?, ?)
                    """,
                    (
                        EventStatus.DRAFTED.value,
                        now,
                        draft.event_id,
                        EventStatus.NOTIFIED.value,
                        EventStatus.SNOOZED.value,
                    ),
                )
                if cursor.rowcount != 1:
                    await db.rollback()
                    return False
                await _insert_draft(db, draft)
                await _insert_feedback(
                    db, draft.event_id, draft.draft_id, FeedbackAction.USE, telegram_user_id, now
                )
        except sqlite3.IntegrityError:
            return False
        return True

    async def save_revision(
        self,
        draft: Draft,
        *,
        previous_id: str,
        action: FeedbackAction,
        telegram_user_id: int,
    ) -> bool:
        now = datetime.now(UTC).isoformat()
        try:
            async with self._transaction() as db:
                cursor = await db.execute(
                    "UPDATE drafts SET status = ? WHERE id = ? AND status = ?",
                    (DraftStatus.SUPERSEDED.value, previous_id, DraftStatus.ACTIVE.value),
                )
                if cursor.rowcount != 1:
                    await db.rollback()
                    return False
                # UNIQUE(event_id, version) and CHECK(version <= 2) make a second revision
                # impossible even if the application check were bypassed.
                await _insert_draft(db, draft)
                await _insert_feedback(
                    db,
                    draft.event_id,
                    draft.draft_id,
                    action,
                    telegram_user_id,
                    now,
                    note=draft.revision_instruction,
                )
        except sqlite3.IntegrityError:
            return False
        return True

    async def mark_verified(self, draft_id: str, *, telegram_user_id: int) -> bool:
        now = datetime.now(UTC).isoformat()
        async with self._transaction() as db:
            cursor = await db.execute(
                """
                UPDATE drafts SET fact_check_status = ?
                WHERE id = ? AND status = ? AND fact_check_status = ?
                """,
                (
                    FactCheckStatus.VERIFIED.value,
                    draft_id,
                    DraftStatus.ACTIVE.value,
                    FactCheckStatus.NEEDS_REVIEW.value,
                ),
            )
            if cursor.rowcount != 1:
                await db.rollback()
                return False
            rows = list(
                await db.execute_fetchall("SELECT event_id FROM drafts WHERE id = ?", (draft_id,))
            )
            await _insert_feedback(
                db, str(rows[0][0]), draft_id, FeedbackAction.VERIFY, telegram_user_id, now
            )
        return True

    async def reject_draft(self, draft: Draft, *, telegram_user_id: int) -> bool:
        now = datetime.now(UTC).isoformat()
        async with self._transaction() as db:
            drafts = await db.execute(
                "UPDATE drafts SET status = ? WHERE id = ? AND status = ?",
                (DraftStatus.REJECTED.value, draft.draft_id, DraftStatus.ACTIVE.value),
            )
            events = await db.execute(
                "UPDATE radar_events SET status = ?, updated_at = ? WHERE id = ? AND status = ?",
                (EventStatus.SKIPPED.value, now, draft.event_id, EventStatus.DRAFTED.value),
            )
            if drafts.rowcount != 1 or events.rowcount != 1:
                await db.rollback()
                return False
            await _insert_feedback(
                db, draft.event_id, draft.draft_id, FeedbackAction.REJECT, telegram_user_id, now
            )
        return True

    async def approve(
        self,
        draft_id: str,
        *,
        telegram_user_id: int,
        build_package: Callable[[EventCandidate, Draft], PublicationPackage],
    ) -> PublicationPackage | None:
        now = datetime.now(UTC).isoformat()
        async with self._transaction() as db:
            latest = list(
                await db.execute_fetchall(
                    """
                    SELECT * FROM drafts
                    WHERE event_id = (SELECT event_id FROM drafts WHERE id = ?)
                    ORDER BY version DESC LIMIT 1
                    """,
                    (draft_id,),
                )
            )
            if not latest or latest[0]["id"] != draft_id:
                await db.rollback()
                return None
            draft = _draft_from_row(latest[0])
            [event_row] = await db.execute_fetchall(
                "SELECT * FROM radar_events WHERE id = ?", (draft.event_id,)
            )
            try:
                package = build_package(self._event_from_row(event_row), draft)
            except ValueError:
                await db.rollback()
                return None

            inserted = await db.execute(
                """
                INSERT INTO publication_outbox (
                    id, event_id, draft_id, payload_json, idempotency_key, status,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(idempotency_key) DO NOTHING
                """,
                (
                    package.package_id,
                    package.event_id,
                    package.draft_id,
                    package.model_dump_json(),
                    package.idempotency_key,
                    package.status.value,
                    now,
                    now,
                ),
            )
            await _insert_feedback(
                db, draft.event_id, draft_id, FeedbackAction.ACCEPT, telegram_user_id, now
            )
            accepted = await db.execute(
                "UPDATE drafts SET status = ? WHERE id = ? AND status = ?",
                (DraftStatus.ACCEPTED.value, draft_id, DraftStatus.ACTIVE.value),
            )
            approved = await db.execute(
                "UPDATE radar_events SET status = ?, updated_at = ? WHERE id = ? AND status = ?",
                (EventStatus.APPROVED.value, now, draft.event_id, EventStatus.DRAFTED.value),
            )
            if accepted.rowcount != 1 or approved.rowcount != 1:
                await db.rollback()
                return None
            if inserted.rowcount == 0:
                [(payload,)] = await db.execute_fetchall(
                    "SELECT payload_json FROM publication_outbox WHERE idempotency_key = ?",
                    (package.idempotency_key,),
                )
                package = PublicationPackage.model_validate_json(payload)
        return package

    async def get_package(self, event_id: str) -> PublicationPackage | None:
        async with self._connect() as db:
            rows = list(
                await db.execute_fetchall(
                    "SELECT payload_json FROM publication_outbox WHERE event_id = ?", (event_id,)
                )
            )
        return PublicationPackage.model_validate_json(rows[0][0]) if rows else None

    async def list_packages(self, status: OutboxStatus, *, limit: int) -> list[PublicationPackage]:
        async with self._connect() as db:
            rows = await db.execute_fetchall(
                """
                SELECT payload_json FROM publication_outbox
                WHERE status = ? ORDER BY created_at DESC LIMIT ?
                """,
                (status.value, limit),
            )
        return [PublicationPackage.model_validate_json(row[0]) for row in rows]

    async def count_packages(self, status: OutboxStatus) -> int:
        async with self._connect() as db:
            [(count,)] = await db.execute_fetchall(
                "SELECT COUNT(*) FROM publication_outbox WHERE status = ?", (status.value,)
            )
        return int(count)

    async def start_run(self, run_id: str) -> None:
        async with self._connect() as db:
            await db.execute(
                "INSERT INTO pipeline_runs (id, started_at, status) VALUES (?, ?, ?)",
                (run_id, datetime.now(UTC).isoformat(), RunStatus.RUNNING.value),
            )
            await db.commit()

    async def finish_run(
        self,
        run_id: str,
        status: RunStatus,
        counters: PipelineCounters,
        error_code: str | None,
    ) -> None:
        async with self._connect() as db:
            await db.execute(
                """
                UPDATE pipeline_runs
                SET finished_at = ?, status = ?, counters_json = ?, error_summary = ?
                WHERE id = ?
                """,
                (
                    datetime.now(UTC).isoformat(),
                    status.value,
                    counters.model_dump_json(),
                    error_code,
                    run_id,
                ),
            )
            await db.commit()

    async def fail_interrupted_runs(self) -> int:
        async with self._connect() as db:
            cursor = await db.execute(
                """
                UPDATE pipeline_runs SET status = ?, finished_at = ?, error_summary = 'interrupted'
                WHERE status = ?
                """,
                (RunStatus.FAILED.value, datetime.now(UTC).isoformat(), RunStatus.RUNNING.value),
            )
            await db.commit()
            return cursor.rowcount

    async def last_run(self, statuses: set[RunStatus] | None = None) -> PipelineRun | None:
        values = sorted(status.value for status in statuses or set(RunStatus))
        async with self._connect() as db:
            rows = list(
                await db.execute_fetchall(
                    f"""
                    SELECT * FROM pipeline_runs WHERE status IN ({_placeholders(values)})
                    ORDER BY started_at DESC LIMIT 1
                    """,
                    values,
                )
            )
        if not rows:
            return None
        row = dict(rows[0])
        return PipelineRun(
            run_id=row["id"],
            status=row["status"],
            started_at=row["started_at"],
            finished_at=row["finished_at"],
            counters=PipelineCounters.model_validate_json(row["counters_json"]),
            error_code=row["error_summary"],
        )

    async def counters_since(self, since: datetime) -> PipelineCounters:
        async with self._connect() as db:
            rows = await db.execute_fetchall(
                "SELECT counters_json FROM pipeline_runs WHERE started_at >= ?",
                (since.astimezone(UTC).isoformat(),),
            )
        total = PipelineCounters()
        for (payload,) in rows:
            counters = PipelineCounters.model_validate_json(payload)
            for name in PipelineCounters.model_fields:
                setattr(total, name, getattr(total, name) + getattr(counters, name))
        return total

    async def reserve_cost(
        self, entry: CostEntry, *, since: datetime, limit_usd: Decimal
    ) -> tuple[int, Decimal] | None:
        cost = _micros(entry.estimated_cost_usd)
        # BEGIN IMMEDIATE takes the write lock before reading the total, so concurrent
        # reservations (other tasks or processes) are serialized and cannot overspend.
        async with self._transaction() as db:
            [(spent,)] = await db.execute_fetchall(
                "SELECT COALESCE(SUM(estimated_cost_micros), 0) FROM cost_ledger "
                "WHERE created_at >= ?",
                (since.astimezone(UTC).isoformat(),),
            )
            if spent + cost > _micros(limit_usd):
                return None
            cursor = await db.execute(
                """
                INSERT INTO cost_ledger (
                    provider, operation, units, estimated_cost_micros, created_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    entry.provider,
                    entry.operation,
                    entry.units,
                    cost,
                    entry.created_at.astimezone(UTC).isoformat(),
                ),
            )
            assert cursor.lastrowid is not None
            return cursor.lastrowid, Decimal(spent + cost) / _MICROS

    async def settle_cost(self, entry_id: int, *, units: int, cost_usd: Decimal) -> None:
        async with self._connect() as db:
            await db.execute(
                """
                UPDATE cost_ledger SET units = ?, estimated_cost_micros = ?
                WHERE id = ? AND estimated_cost_micros >= ?
                """,
                (units, _micros(cost_usd), entry_id, _micros(cost_usd)),
            )
            await db.commit()

    async def cost_since(self, since: datetime) -> Decimal:
        async with self._connect() as db:
            [(spent,)] = await db.execute_fetchall(
                "SELECT COALESCE(SUM(estimated_cost_micros), 0) FROM cost_ledger "
                "WHERE created_at >= ?",
                (since.astimezone(UTC).isoformat(),),
            )
        return Decimal(spent) / _MICROS

    async def source_health(self) -> list[SourceHealth]:
        async with self._connect() as db:
            rows = await db.execute_fetchall("SELECT * FROM source_checkpoints ORDER BY source_key")
        return [
            SourceHealth.model_validate(
                {k: v for k, v in dict(row).items() if k != "cursor_value"}
                | {"blocked_gaps": _blocked_gaps(row["cursor_value"])}
            )
            for row in rows
        ]

    async def edit_cursor(
        self, source_key: str, edit: Callable[[str | None], str | None]
    ) -> str | None:
        """Read-modify-write one cursor in a single write transaction (operator commands)."""
        async with self._transaction() as db:
            rows = list(
                await db.execute_fetchall(
                    "SELECT cursor_value FROM source_checkpoints WHERE source_key = ?",
                    (source_key,),
                )
            )
            value = edit(str(rows[0][0]) if rows and rows[0][0] is not None else None)
            await db.execute(
                "UPDATE source_checkpoints SET cursor_value = ? WHERE source_key = ?",
                (value, source_key),
            )
        return value

    # --- selection: mentions, rejected samples, clusters -------------------------------------

    async def add_rejected_samples(self, samples: Sequence[RejectedSample]) -> None:
        if not samples:
            return
        async with self._transaction() as db:
            await db.executemany(
                "INSERT INTO rejected_samples (reason, source_key, text, url, seen_at) "
                "VALUES (?, ?, ?, ?, ?)",
                [
                    (s.reason, s.source_key, s.text[:REJECTED_TEXT_CHARS], s.url, _iso(s.seen_at))
                    for s in samples
                ],
            )
            # A sample, not a log: only the newest rows per reason are kept.
            for reason in {sample.reason for sample in samples}:
                await db.execute(
                    """
                    DELETE FROM rejected_samples WHERE reason = ? AND id <= (
                        SELECT id FROM rejected_samples WHERE reason = ?
                        ORDER BY id DESC LIMIT 1 OFFSET ?
                    )
                    """,
                    (reason, reason, REJECTED_SAMPLES_PER_REASON),
                )

    async def unclustered_events(self, *, limit: int) -> list[EventCandidate]:
        async with self._connect() as db:
            rows = await db.execute_fetchall(
                """
                SELECT * FROM radar_events INDEXED BY idx_events_unclustered
                WHERE cluster_id IS NULL AND status = 'DISCOVERED'
                  AND COALESCE(source_key, '') != 'manual'
                ORDER BY discovered_at, rowid LIMIT ?
                """,
                (limit,),
            )
        return [self._event_from_row(row) for row in rows]

    async def representatives(
        self, band_keys: Sequence[int], *, seen_since: datetime
    ) -> dict[int, list[Representative]]:
        found: dict[int, list[Representative]] = {}
        async with self._connect() as db:
            for chunk in _chunks(sorted(set(band_keys))):
                rows = await db.execute_fetchall(
                    f"""
                    SELECT k.band_key, c.id, e.id, e.original_text, e.language
                    FROM cluster_keys k
                    JOIN event_clusters c ON c.id = k.cluster_id
                    JOIN radar_events e ON e.id = c.representative_event_id
                    WHERE k.band_key IN ({_placeholders(chunk)}) AND c.last_seen_at >= ?
                    """,
                    (*chunk, _iso(seen_since)),
                )
                for key, cluster_id, event_id, text, language in rows:
                    found.setdefault(int(key), []).append(
                        Representative(int(cluster_id), str(event_id), str(text), language)
                    )
        return found

    async def save_clustering(
        self,
        new_clusters: Sequence[tuple[EventCandidate, Sequence[int]]],
        joins: Sequence[ClusterJoin],
        *,
        now: datetime,
    ) -> int:
        stamp = _iso(now)
        # One statement per kind of row, not per story: a round trip costs more than the insert.
        async with self._transaction() as db:
            await db.executemany(
                """
                INSERT INTO event_clusters (
                    representative_event_id, language, first_seen_at, last_seen_at,
                    created_at, updated_at, article
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        event.event_id,
                        event.language,
                        _iso(event.discovered_at),
                        _iso(event.discovered_at),
                        stamp,
                        stamp,
                        article_of(event),
                    )
                    for event, _ in new_clusters
                ],
            )
            await db.executemany(
                f"UPDATE radar_events SET cluster_id = ({_CLUSTER_OF}), updated_at = ? "
                "WHERE id = ?",
                [(event.event_id, stamp, event.event_id) for event, _ in new_clusters],
            )
            await db.executemany(
                f"INSERT OR IGNORE INTO cluster_keys (band_key, cluster_id) "
                f"SELECT ?, ({_CLUSTER_OF})",
                [(key, event.event_id) for event, keys in new_clusters for key in keys],
            )
            before = db.total_changes
            await db.executemany(
                f"""
                UPDATE radar_events SET
                    cluster_id = ({_CLUSTER_OF}), status = ?, filter_reason = ?,
                    duplicate_of_event_id = ?, updated_at = ?
                WHERE id = ? AND status = ? AND cluster_id IS NULL
                """,
                [
                    (
                        join.representative_event_id,
                        EventStatus.FILTERED_OUT.value,
                        join.reason,
                        join.representative_event_id,
                        stamp,
                        join.event_id,
                        EventStatus.DISCOVERED.value,
                    )
                    for join in joins
                ],
            )
            attached = db.total_changes - before
            await db.executemany(
                "UPDATE event_clusters SET preselect_score = -1 WHERE representative_event_id = ?",
                [
                    (representative,)
                    for representative in {j.representative_event_id for j in joins}
                ],
            )
            return attached

    async def clusters_of(self, event_ids: Sequence[str]) -> set[int]:
        found: set[int] = set()
        async with self._connect() as db:
            for chunk in _chunks(sorted(set(event_ids))):
                rows = await db.execute_fetchall(
                    f"SELECT DISTINCT cluster_id FROM radar_events "
                    f"WHERE id IN ({_placeholders(chunk)}) AND cluster_id IS NOT NULL",
                    chunk,
                )
                found.update(int(row[0]) for row in rows)
        return found

    async def unscored_clusters(self, *, limit: int) -> set[int]:
        async with self._connect() as db:
            rows = await db.execute_fetchall(
                "SELECT id FROM event_clusters INDEXED BY idx_clusters_dirty "
                "WHERE preselect_score = -1 ORDER BY id LIMIT ?",
                (limit,),
            )
        return {int(row[0]) for row in rows}

    async def refresh_clusters(
        self, cluster_ids: Sequence[int], *, now: datetime
    ) -> list[ClusterSignals]:
        stamp = _iso(now)
        async with self._transaction() as db:
            for chunk in _chunks(sorted(set(cluster_ids))):
                sightings = _SIGHTINGS.format(clusters=_placeholders(chunk))
                await db.execute(
                    f"""
                    UPDATE event_clusters SET
                        mention_count = a.mentions, domain_count = a.domains,
                        article_count = a.articles,
                        source_count = a.sources, member_count = a.members,
                        first_seen_at = a.first_seen, last_seen_at = a.last_seen, updated_at = ?
                    FROM (
                        SELECT cluster AS id, COUNT(*) AS mentions,
                               COUNT(DISTINCT domain) AS domains,
                               COUNT(DISTINCT article) AS articles,
                               COUNT(DISTINCT COALESCE(source_key, '')) AS sources,
                               COUNT(DISTINCT text_id) AS members,
                               MIN(seen_at) AS first_seen, MAX(seen_at) AS last_seen
                        FROM ({sightings}) GROUP BY cluster
                    ) AS a
                    WHERE event_clusters.id = a.id
                    """,
                    (stamp, *chunk, *chunk),
                )
        return await self._signals("c.id", cluster_ids, now=now)

    async def cluster_signals(
        self, event_ids: Sequence[str], *, now: datetime
    ) -> list[ClusterSignals]:
        return await self._signals("c.representative_event_id", event_ids, now=now)

    async def _signals(
        self, column: str, values: Sequence[str | int], *, now: datetime
    ) -> list[ClusterSignals]:
        hour_ago = _iso(now - timedelta(hours=1))
        result: list[ClusterSignals] = []
        async with self._connect() as db:
            for chunk in _chunks(sorted(set(values), key=str)):
                rows = list(
                    await db.execute_fetchall(
                        f"""
                        SELECT e.*, c.id AS c_id, c.state AS c_state, c.mention_count,
                               c.domain_count,
                               c.article_count, c.source_count, c.member_count, c.first_seen_at
                        FROM event_clusters c
                        JOIN radar_events e ON e.id = c.representative_event_id
                        WHERE {column} IN ({_placeholders(chunk)})
                        """,
                        chunk,
                    )
                )
                ids = [int(row["c_id"]) for row in rows]
                recent: dict[int, int] = {}
                domains: dict[int, list[str]] = {}
                for part in _chunks(ids):
                    sightings = _SIGHTINGS.format(clusters=_placeholders(part))
                    for cluster, count in await db.execute_fetchall(
                        # Distinct articles, not sightings: a burst of reprints is one article.
                        f"SELECT cluster, COUNT(DISTINCT article) FROM ({sightings}) "
                        f"WHERE seen_at >= ? GROUP BY cluster",
                        (*part, *part, hour_ago),
                    ):
                        recent[int(cluster)] = int(count)
                    for cluster, domain in await db.execute_fetchall(
                        f"SELECT cluster, domain FROM ({sightings}) WHERE domain IS NOT NULL "
                        f"GROUP BY cluster, domain ORDER BY cluster, COUNT(*) DESC, domain",
                        (*part, *part),
                    ):
                        domains.setdefault(int(cluster), []).append(str(domain))
                for row in rows:
                    cluster_id = int(row["c_id"])
                    result.append(
                        ClusterSignals(
                            event=self._event_from_row(row),
                            cluster_id=cluster_id,
                            mentions=int(row["mention_count"]),
                            domains=int(row["domain_count"]),
                            articles=int(row["article_count"]),
                            sources=int(row["source_count"]),
                            members=int(row["member_count"]),
                            first_seen=datetime.fromisoformat(row["first_seen_at"]),
                            articles_last_hour=recent.get(cluster_id, 0),
                            sample_domains=tuple(domains.get(cluster_id, [])[:3]),
                            state=str(row["c_state"]),
                        )
                    )
        return result

    async def save_preselection(self, scores: Sequence[ClusterScore]) -> None:
        if not scores:
            return
        async with self._transaction() as db:
            await db.executemany(
                """
                UPDATE event_clusters SET state = ?, preselect_score = ?, preselect_json = ?
                WHERE id = ?
                """,
                [
                    (
                        score.state,
                        score.score,
                        json.dumps(score.details, ensure_ascii=False),
                        score.cluster_id,
                    )
                    for score in scores
                ],
            )

    async def preselected_articles(self, articles: Sequence[str]) -> dict[str, int]:
        found: dict[str, int] = {}
        async with self._connect() as db:
            for chunk in _chunks(sorted(set(articles))):
                rows = await db.execute_fetchall(
                    f"""
                    SELECT article, MIN(id) FROM event_clusters
                    WHERE state = 'preselected' AND article IN ({_placeholders(chunk)})
                    GROUP BY article
                    """,
                    chunk,
                )
                found.update((str(article), int(cluster)) for article, cluster in rows)
        return found

    async def reopen_archived(self, event_ids: Sequence[str]) -> int:
        reopened = 0
        async with self._transaction() as db:
            for chunk in _chunks(sorted(set(event_ids))):
                # Only a ranking archived these; anything a person touched has another status.
                cursor = await db.execute(
                    f"""
                    UPDATE radar_events SET status = ?, updated_at = ?
                    WHERE id IN ({_placeholders(chunk)}) AND status = ?
                      AND NOT EXISTS (SELECT 1 FROM feedback f WHERE f.event_id = radar_events.id)
                    """,
                    (
                        EventStatus.DISCOVERED.value,
                        _iso(datetime.now(UTC)),
                        *chunk,
                        EventStatus.ARCHIVED.value,
                    ),
                )
                reopened += cursor.rowcount
        return reopened

    async def rank_candidates(self, *, limit: int) -> list[EventCandidate]:
        async with self._connect() as db:
            rows = await db.execute_fetchall(
                """
                SELECT * FROM (
                    SELECT e.*, 1000 AS priority, e.rowid AS position FROM radar_events e
                    WHERE e.status = 'DISCOVERED' AND e.source_key = 'manual'
                    UNION ALL
                    SELECT e.*, c.preselect_score AS priority, e.rowid AS position
                    FROM event_clusters c
                    JOIN radar_events e ON e.id = c.representative_event_id
                    WHERE c.state = 'preselected' AND e.status = 'DISCOVERED'
                )
                ORDER BY priority DESC, position DESC LIMIT ?
                """,
                (limit,),
            )
        return [self._event_from_row(row) for row in rows]

    # --- audit (read-only) and retention -----------------------------------------------------

    @asynccontextmanager
    async def _read(self) -> AsyncIterator[aiosqlite.Connection]:
        """Read-only connection for audit commands: can neither migrate nor change anything."""
        async with aiosqlite.connect(f"{self._db_path.resolve().as_uri()}?mode=ro", uri=True) as db:
            db.row_factory = aiosqlite.Row
            yield db

    async def list_clusters(
        self, *, limit: int, state: str | None, order: str
    ) -> list[dict[str, object]]:
        orders = {
            "score": "c.preselect_score DESC, c.domain_count DESC",
            "mentions": "c.mention_count DESC",
            "recent": "c.id DESC",
        }
        async with self._read() as db:
            rows = await db.execute_fetchall(
                f"""
                SELECT c.id, c.state, c.preselect_score, c.mention_count, c.domain_count,
                       c.article_count, c.source_count, c.member_count, c.first_seen_at,
                       c.last_seen_at,
                       e.status, e.original_text, e.url, e.source_key,
                       s.total AS rank_total
                FROM event_clusters c
                JOIN radar_events e ON e.id = c.representative_event_id
                LEFT JOIN event_scores s ON s.event_id = e.id
                WHERE (? IS NULL OR c.state = ?)
                ORDER BY {orders[order]} LIMIT ?
                """,
                (state, state, limit),
            )
        return [dict(row) for row in rows]

    async def cluster_detail(self, cluster_id: int, *, limit: int) -> dict[str, object] | None:
        async with self._read() as db:
            found = list(
                await db.execute_fetchall(
                    """
                    SELECT c.*, e.original_text, e.url, e.status, e.source_key,
                           s.total AS rank_total, s.rationale AS rank_rationale
                    FROM event_clusters c
                    JOIN radar_events e ON e.id = c.representative_event_id
                    LEFT JOIN event_scores s ON s.event_id = e.id
                    WHERE c.id = ?
                    """,
                    (cluster_id,),
                )
            )
            if not found:
                return None
            members = await db.execute_fetchall(
                """
                SELECT id, source_key, status, filter_reason, original_text, url, discovered_at
                FROM radar_events WHERE cluster_id = ? ORDER BY rowid LIMIT ?
                """,
                (cluster_id, limit),
            )
            sightings = _SIGHTINGS.format(clusters="?")
            mentions = await db.execute_fetchall(
                f"SELECT domain, url, source_key, seen_at, text_id FROM ({sightings}) "
                f"ORDER BY seen_at LIMIT ?",
                (cluster_id, cluster_id, limit),
            )
            domains = await db.execute_fetchall(
                f"SELECT domain, COUNT(*) AS mentions FROM ({sightings}) "
                f"GROUP BY domain ORDER BY mentions DESC, domain LIMIT ?",
                (cluster_id, cluster_id, limit),
            )
        detail = dict(found[0])
        detail["preselect"] = json.loads(str(detail.pop("preselect_json")))
        detail["members"] = [dict(row) for row in members]
        detail["mentions"] = [dict(row) for row in mentions]
        detail["domains"] = {str(row[0]): int(row[1]) for row in domains}
        return detail

    async def rejected_samples(self, *, reason: str | None, limit: int) -> list[dict[str, object]]:
        async with self._read() as db:
            rows = await db.execute_fetchall(
                """
                SELECT reason, source_key, text, url, seen_at FROM rejected_samples
                WHERE (? IS NULL OR reason = ?) ORDER BY RANDOM() LIMIT ?
                """,
                (reason, reason, limit),
            )
        return [dict(row) for row in rows]

    async def selection_counts(self) -> dict[str, dict[str, int]]:
        """Current state of stored stories and events, for `qmemo-radar funnel`."""
        async with self._read() as db:
            clusters = await db.execute_fetchall(
                "SELECT state, COUNT(*) FROM event_clusters GROUP BY state"
            )
            multi = await db.execute_fetchall(
                """
                SELECT SUM(member_count > 1), SUM(domain_count > 1), SUM(mention_count),
                       COUNT(*) FROM event_clusters
                """
            )
            events = await db.execute_fetchall(
                "SELECT status, COUNT(*) FROM radar_events GROUP BY status"
            )
            reasons = await db.execute_fetchall(
                """
                SELECT COALESCE(filter_reason, ''), COUNT(*) FROM radar_events
                WHERE status = 'FILTERED_OUT' GROUP BY 1
                """
            )
            ranked = await db.execute_fetchall(
                """
                SELECT e.status, COUNT(*) FROM event_scores s
                JOIN radar_events e ON e.id = s.event_id GROUP BY e.status
                """
            )
        [(with_texts, with_domains, mentions, total)] = list(multi)
        return {
            "clusters_by_state": {str(state): int(count) for state, count in clusters},
            "clusters": {
                "total": int(total or 0),
                "with_several_texts": int(with_texts or 0),
                "with_several_domains": int(with_domains or 0),
                "mentions": int(mentions or 0),
            },
            "events_by_status": {str(status): int(count) for status, count in events},
            "filtered_by_reason": {str(reason): int(count) for reason, count in reasons},
            "ranked_by_status": {str(status): int(count) for status, count in ranked},
        }

    async def prune(self, cutoffs: PruneCutoffs, *, apply: bool) -> dict[str, int]:
        """Counts (and with apply=True deletes) what retention allows, per category.

        Never touched: anything a person saw or acted on (Telegram delivery, draft, feedback,
        outbox package), the stories and copies of such events, and everything newer than the
        cutoffs. Deleting an event deletes its score, mentions and, for a representative, its
        story (foreign keys). The count only reads; deleting goes in short transactions of
        PRUNE_BATCH rows, so a running collection is never locked out for long.
        """
        evidence = _iso(cutoffs.evidence_before)
        noise = _iso(cutoffs.noise_before)
        members = f"""
            SELECT e.id FROM radar_events e
            JOIN event_clusters c ON c.id = e.cluster_id
            WHERE e.filter_reason IN ('near_duplicate', 'same_event')
              AND e.discovered_at < ? AND NOT {_HUMAN.format(event="e.id")}
              AND NOT {_HUMAN.format(event="c.representative_event_id")}
        """
        copies = f"""
            SELECT m.event_id, m.url FROM content_mentions m
            JOIN radar_events e ON e.id = m.event_id
            LEFT JOIN event_clusters c ON c.id = e.cluster_id
            WHERE m.seen_at < ? AND m.url != e.url
              AND NOT {_HUMAN.format(event="e.id")}
              AND (c.id IS NULL OR NOT {_HUMAN.format(event="c.representative_event_id")})
        """
        statuses = ",".join(f"'{status.value}'" for status in _NOISE)
        # A row still referenced as the original of another stays; variants deleted above do not
        # count (the count, which deletes nothing, must see what the deletion will see).
        noise_rows = f"""
            SELECT e.id FROM radar_events e
            WHERE e.status IN ({statuses}) AND e.discovered_at < ?
              AND COALESCE(e.filter_reason, '') NOT IN ('near_duplicate', 'same_event')
              AND NOT {_HUMAN.format(event="e.id")}
              AND NOT EXISTS (
                  SELECT 1 FROM radar_events x WHERE x.duplicate_of_event_id = e.id
                  AND x.id NOT IN ({members})
              )
        """
        stories = f"""
            SELECT c.id FROM event_clusters c WHERE c.representative_event_id IN ({noise_rows})
        """
        samples = "SELECT id FROM rejected_samples WHERE seen_at < ?"
        # Keys only find stories active within the clustering window, so older keys are dead
        # weight; the keys of stories deleted with their representative go too (no foreign key).
        stale_keys = """
            SELECT band_key, cluster_id FROM cluster_keys WHERE cluster_id NOT IN (
                SELECT id FROM event_clusters WHERE last_seen_at >= ?
            )
        """
        metrics = "SELECT run_id, source_key, metric FROM pipeline_metrics WHERE recorded_at < ?"
        runs = "SELECT id FROM pipeline_runs WHERE started_at < ? AND status != 'RUNNING'"
        index, kept = _iso(cutoffs.index_before), _iso(cutoffs.metrics_before)
        categories: tuple[tuple[str, str, tuple[str, ...], str | None, tuple[str, ...]], ...] = (
            ("duplicate_texts", members, (evidence,), "radar_events", ("id",)),
            ("copy_mentions", copies, (evidence,), "content_mentions", ("event_id", "url")),
            ("rejected_samples", samples, (noise,), "rejected_samples", ("id",)),
            ("stories", stories, (noise, evidence), None, ()),
            ("noise_events", noise_rows, (noise, evidence), "radar_events", ("id",)),
            ("band_keys", stale_keys, (index,), "cluster_keys", ("band_key", "cluster_id")),
            ("metrics", metrics, (kept,), "pipeline_metrics", ("run_id", "source_key", "metric")),
            ("runs", runs, (kept,), "pipeline_runs", ("id",)),
        )
        counts: dict[str, int] = {}
        for name, query, values, table, key in categories:
            async with self._connect() as db:
                keys = [tuple(row) for row in await db.execute_fetchall(query, values)]
            counts[name] = len(keys)
            if not apply or table is None:
                continue
            columns = f"({', '.join(key)})" if len(key) > 1 else key[0]
            for start in range(0, len(keys), PRUNE_BATCH):
                chunk = keys[start : start + PRUNE_BATCH]
                rows = ",".join(f"({', '.join('?' for _ in key)})" for _ in chunk)
                async with self._transaction() as db:
                    await db.execute(
                        f"DELETE FROM {table} WHERE {columns} IN (VALUES {rows})",
                        [value for row in chunk for value in row],
                    )
        return counts

    @classmethod
    def _scored_from_row(cls, row: aiosqlite.Row) -> ScoredEvent:
        values = dict(row)
        breakdown = ScoreBreakdown.model_validate(
            {name: values[name] for name in ScoreBreakdown.model_fields}
        )
        score = ScoreResult(
            event_id=values["event_id"],
            breakdown=breakdown,
            total=values["total"],
            rationale=values["rationale"],
            recommended_format=values["recommended_format"],
            target_action=values["target_action"],
            fact_check_required=bool(values["fact_check_required"]),
            fact_check_note=values["fact_check_note"],
            prompt_version=values["prompt_version"],
            model_name=values["model_name"],
            headline=values["headline"],
            summary=values["summary"],
        )
        return ScoredEvent(event=cls._event_from_row(row), score=score)

    @staticmethod
    def _event_from_row(row: aiosqlite.Row) -> EventCandidate:
        values = dict(row)
        return EventCandidate.model_validate(
            {
                "event_id": values["id"],
                "source": values["source"],
                "external_id": values["external_id"],
                "url": values["url"],
                "author_id": values["author_id"],
                "author_handle": values["author_handle"],
                "author_display_name": values["author_display_name"],
                "original_text": values["original_text"],
                "normalized_text": values["normalized_text"]
                or comparison_text(values["original_text"]),
                "content_hash": values["content_hash"],
                "language": values["language"],
                "published_at": values["published_at"],
                "discovered_at": values["discovered_at"],
                "engagement": Engagement.model_validate_json(values["engagement_json"]),
                "raw_payload": json.loads(values["raw_payload_json"]),
                "status": values["status"],
                "source_key": values["source_key"],
                "filter_reason": values["filter_reason"],
                "duplicate_of_event_id": values["duplicate_of_event_id"],
            }
        )

    @asynccontextmanager
    async def _transaction(self) -> AsyncIterator[aiosqlite.Connection]:
        """BEGIN IMMEDIATE ... COMMIT; any exception rolls everything back."""
        async with self._connect() as db:
            await db.execute("BEGIN IMMEDIATE")
            try:
                yield db
            except BaseException:
                await db.rollback()
                raise
            await db.commit()

    @asynccontextmanager
    async def _connect(self) -> AsyncIterator[aiosqlite.Connection]:
        db = await aiosqlite.connect(self._db_path)
        try:
            db.row_factory = aiosqlite.Row
            await db.execute("PRAGMA journal_mode = WAL")
            await db.execute("PRAGMA foreign_keys = ON")
            await db.execute("PRAGMA busy_timeout = 5000")
            await db.execute("PRAGMA synchronous = NORMAL")
            # 64 MB page cache instead of 2 MB: batched inserts into large random-key indexes
            # (event ids, mentions) measured 3-5x faster at a few hundred thousand rows.
            await db.execute("PRAGMA cache_size = -65536")
            yield db
        finally:
            await db.close()


_MICROS = 1_000_000
REJECTED_TEXT_CHARS = 300
# Rows deleted per prune transaction: the write lock is held for well under a second.
PRUNE_BATCH = 1_000
# Rows that never own a text: legacy copies, and texts filtered for the item, not the text.
_NOT_OWNERS = ("duplicate_content", *sorted(ITEM_REASONS))
# Every sighting of the stories in {clusters}: each stored text at its own URL, and each exact
# copy of it elsewhere. Columns: cluster, text, domain, source key, time.
_SIGHTINGS = """
    SELECT e.cluster_id AS cluster, e.id AS text_id, e.domain AS domain,
           e.source_key AS source_key, e.discovered_at AS seen_at, e.url AS url,
           e.article AS article
    FROM radar_events e WHERE e.cluster_id IN ({clusters})
    UNION ALL
    SELECT e.cluster_id, m.event_id, m.domain, m.source_key, m.seen_at, m.url, m.article
    FROM content_mentions m JOIN radar_events e ON e.id = m.event_id
    WHERE e.cluster_id IN ({clusters})
"""
_HUMAN = """EXISTS (
    SELECT 1 FROM telegram_deliveries d WHERE d.event_id = {event}
    UNION ALL SELECT 1 FROM drafts r WHERE r.event_id = {event}
    UNION ALL SELECT 1 FROM feedback f WHERE f.event_id = {event}
    UNION ALL SELECT 1 FROM publication_outbox o WHERE o.event_id = {event}
)"""
_CLUSTER_OF = "SELECT id FROM event_clusters WHERE representative_event_id = ?"
REJECTED_SAMPLES_PER_REASON = 500
# SQLite accepts 32k bound parameters; far below that keeps every IN list cheap to plan.
_CHUNK = 500


def _iso(moment: datetime) -> str:
    return moment.astimezone(UTC).isoformat()


def _chunks[T](values: Sequence[T]) -> list[Sequence[T]]:
    return [values[index : index + _CHUNK] for index in range(0, len(values), _CHUNK)]


def _micros(usd: Decimal) -> int:
    """Round up: an estimate may only err on the expensive side."""
    return int((usd * _MICROS).to_integral_value(rounding=ROUND_CEILING))


def _event_row(event: EventCandidate, now: str) -> tuple[object, ...]:
    return (
        event.event_id,
        event.source.value,
        event.external_id,
        str(event.url),
        event.author_id,
        event.author_handle,
        event.author_display_name,
        event.original_text,
        # Derived from original_text on read (comparison_text): storing it doubled the text.
        "",
        event.content_hash,
        event.language,
        event.published_at.isoformat(),
        event.discovered_at.isoformat(),
        event.engagement.model_dump_json(),
        json.dumps(event.raw_payload, ensure_ascii=False, default=str),
        event.status.value,
        now,
        now,
        event.source_key,
        event.filter_reason,
        domain_of(str(event.url)),
        article_of(event),
        event.duplicate_of_event_id,
    )


def _blocked_gaps(cursor: object) -> int:
    """A source keeps parked failures under `blocked` in a JSON cursor (see GdeltCursor)."""
    if not isinstance(cursor, str) or not cursor.startswith("{"):
        return 0
    try:
        blocked = json.loads(cursor).get("blocked")
    except (ValueError, AttributeError):
        return 0
    return len(blocked) if isinstance(blocked, dict) else 0


def _placeholders(values: Sequence[object]) -> str:
    return ",".join("?" for _ in values)


async def _insert_feedback(
    db: aiosqlite.Connection,
    event_id: str,
    draft_id: str | None,
    action: FeedbackAction,
    telegram_user_id: int,
    now: str,
    note: str | None = None,
) -> None:
    await db.execute(
        """
        INSERT INTO feedback (event_id, draft_id, action, note, telegram_user_id, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (event_id, draft_id, action.value, note, telegram_user_id, now),
    )


async def _insert_draft(db: aiosqlite.Connection, draft: Draft) -> None:
    await db.execute(
        """
        INSERT INTO drafts (
            id, event_id, version, quote_text, quote_author, quote_language,
            context_summary, qmemo_text, x_text_template, x_text_short, angle, cta,
            fact_check_status, fact_check_notes_json, prompt_version, model_name,
            revision_instruction, status, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            draft.draft_id,
            draft.event_id,
            draft.version,
            draft.quote_text,
            draft.quote_author,
            draft.quote_language,
            draft.context_summary,
            draft.qmemo_text,
            draft.x_text_template,
            draft.x_text_short,
            draft.angle,
            draft.cta,
            draft.fact_check_status.value,
            json.dumps(list(draft.fact_check_notes), ensure_ascii=False),
            draft.prompt_version,
            draft.model_name,
            draft.revision_instruction,
            draft.status.value,
            draft.created_at.isoformat(),
        ),
    )


def _draft_from_row(row: aiosqlite.Row) -> Draft:
    values = dict(row)
    return Draft.model_validate(
        {
            **{key: values[key] for key in Draft.model_fields if key in values},
            "draft_id": values["id"],
            "fact_check_notes": tuple(json.loads(values["fact_check_notes_json"])),
        }
    )

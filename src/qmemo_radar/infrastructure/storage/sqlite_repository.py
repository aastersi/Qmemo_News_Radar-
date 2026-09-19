import json
import sqlite3
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from decimal import ROUND_CEILING, Decimal
from importlib.resources import files
from pathlib import Path

import aiosqlite

from qmemo_radar.application.ports import KnownEvents
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
        source_key, filter_reason, duplicate_of_event_id
    ) VALUES (
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
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

    async def add_events(self, events: Sequence[EventCandidate]) -> int:
        if not events:
            return 0
        now = datetime.now(UTC).isoformat()
        async with self._transaction() as db:
            before = db.total_changes
            await db.executemany(_INSERT_EVENT, [_event_row(event, now) for event in events])
            return db.total_changes - before

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
                      AND COALESCE(filter_reason, '') != 'duplicate_content'
                    GROUP BY content_hash
                    """,
                    hashes,
                )
                owners = {str(row[0]): str(row[1]) for row in rows}
        return KnownEvents(ids=frozenset(ids), urls=frozenset(urls), content_owners=owners)

    async def count_prunable_noise(self, discovered_before: datetime) -> dict[str, int]:
        values = [status.value for status in _NOISE]
        async with self._connect() as db:
            rows = await db.execute_fetchall(
                f"""
                SELECT e.status, COUNT(*) FROM radar_events e
                WHERE e.status IN ({_placeholders(values)}) AND e.discovered_at < ?
                  AND NOT EXISTS (SELECT 1 FROM telegram_deliveries d WHERE d.event_id = e.id)
                  AND NOT EXISTS (SELECT 1 FROM drafts r WHERE r.event_id = e.id)
                  AND NOT EXISTS (SELECT 1 FROM feedback f WHERE f.event_id = e.id)
                  AND NOT EXISTS (SELECT 1 FROM publication_outbox o WHERE o.event_id = e.id)
                  AND NOT EXISTS (
                      SELECT 1 FROM radar_events c WHERE c.duplicate_of_event_id = e.id
                  )
                GROUP BY e.status
                """,
                (*values, discovered_before.astimezone(UTC).isoformat()),
            )
        return {str(status): int(count) for status, count in rows}

    async def record_metrics(
        self, run_id: str, metrics: Mapping[str, Mapping[str, int]]
    ) -> None:
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
            rows = await db.execute_fetchall(
                """
                SELECT 1 FROM radar_events
                WHERE content_hash = ?
                  AND rowid < (SELECT rowid FROM radar_events WHERE id = ?)
                  AND COALESCE(filter_reason, '') != 'duplicate_content'
                LIMIT 1
                """,
                (event.content_hash, event.event_id),
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
                "normalized_text": values["normalized_text"],
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
            yield db
        finally:
            await db.close()


_MICROS = 1_000_000


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
        event.normalized_text,
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


def _placeholders(values: Sequence[str]) -> str:
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

import argparse
import asyncio
import itertools
import json
import sys
import time
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path

from pydantic import HttpUrl

from qmemo_radar.application.budget import month_start
from qmemo_radar.application.ports import PruneCutoffs
from qmemo_radar.application.runner import HEARTBEAT_KEY
from qmemo_radar.bootstrap import build_application, build_services, enabled_sources
from qmemo_radar.config import RadarSettings, SelectionConfig, SourcesConfig, load_sources
from qmemo_radar.domain import (
    Engagement,
    FactCheckStatus,
    OutboxStatus,
    RawSourceItem,
    ScoredEvent,
    SourceType,
)
from qmemo_radar.infrastructure.collectors import FakeCollector
from qmemo_radar.infrastructure.collectors.gdelt_gqg import SOURCE_KEY as GDELT_KEY
from qmemo_radar.infrastructure.collectors.gdelt_gqg import GdeltCursor
from qmemo_radar.infrastructure.drafting import DeterministicDraftWriter
from qmemo_radar.infrastructure.ranking import DeterministicFixtureRanker
from qmemo_radar.infrastructure.storage import SQLiteEventRepository

HEARTBEAT_MAX_AGE = timedelta(minutes=3)
SAMPLE_MAX = 100
SAMPLE_TEXT_CHARS = 500
DRY_RUN_USER_ID = 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="qmemo-radar")
    parser.add_argument(
        "command",
        choices=(
            "init-db",
            "status",
            "dry-run",
            "run",
            "healthcheck",
            "check-config",
            "sample",
            "gaps",
            "funnel",
            "clusters",
            "cluster",
            "rejected",
            "prune",
        ),
    )
    parser.add_argument("target", nargs="?", help="cluster: the cluster id")
    parser.add_argument("--db", type=Path, help="Override SQLite path")
    parser.add_argument(
        "--source", default="gdelt", help="sample: source (gdelt, rss, x) or key (rss:wire)"
    )
    parser.add_argument(
        "--limit", type=_sample_limit, default=20, help=f"sample: 1-{SAMPLE_MAX} items"
    )
    parser.add_argument("--random", action="store_true", help="sample: random instead of newest")
    parser.add_argument("--hours", type=int, default=24, help="funnel: period (1-720 hours)")
    parser.add_argument(
        "--state",
        choices=("preselected", "candidate", "sibling", "boilerplate"),
        help="clusters: filter",
    )
    parser.add_argument(
        "--order", choices=("score", "mentions", "recent"), default="score", help="clusters"
    )
    parser.add_argument("--reason", help="rejected: one gate reason, e.g. too_few_words")
    parser.add_argument("--apply", action="store_true", help="prune: really delete")
    parser.add_argument(
        "--skip",
        nargs="+",
        metavar="MINUTE",
        help="gaps: give up on blocked GDELT minutes (YYYYMMDDHHMMSS, or all); recorded",
    )
    return parser


def _sample_limit(value: str) -> int:
    number = int(value)
    if not 1 <= number <= SAMPLE_MAX:
        raise argparse.ArgumentTypeError(f"must be between 1 and {SAMPLE_MAX}")
    return number


async def execute(
    command: str,
    *,
    db_path: Path | None = None,
    source: str = "gdelt",
    limit: int = 20,
    random: bool = False,
    skip: list[str] | None = None,
    target: str | None = None,
    hours: int = 24,
    state: str | None = None,
    order: str = "score",
    reason: str | None = None,
    apply: bool = False,
) -> int:
    settings = RadarSettings(db_path=db_path) if db_path else RadarSettings()

    if command == "gaps":
        return await _gaps(settings, skip=skip)

    if command in ("funnel", "clusters", "cluster", "rejected"):
        # Read-only audit: never creates, migrates or changes the database.
        if not settings.db_path.is_file():
            return _print(
                {"status": "error", "error": f"database not found: {settings.db_path}"}, 1
            )
        repository = SQLiteEventRepository(settings.db_path)
        limit = min(limit, SAMPLE_MAX)
        if command == "funnel":
            return await _funnel(repository, hours=max(1, min(hours, 720)))
        if command == "clusters":
            rows = await repository.list_clusters(limit=limit, state=state, order=order)
            return _print({"status": "ok", "count": len(rows), "clusters": _clip(rows)})
        if command == "cluster":
            if not (target or "").isdigit():
                return _print({"status": "error", "error": "usage: qmemo-radar cluster <id>"}, 2)
            detail = await repository.cluster_detail(int(str(target)), limit=limit)
            if detail is None:
                return _print({"status": "error", "error": f"cluster {target} not found"}, 1)
            return _print({"status": "ok", "cluster": detail})
        rows = await repository.rejected_samples(reason=reason, limit=limit)
        return _print({"status": "ok", "count": len(rows), "samples": rows})

    if command == "prune":
        repository = SQLiteEventRepository(settings.db_path)
        await repository.initialize()
        now = datetime.now(UTC)
        counts = await repository.prune(_prune_cutoffs(settings, now), apply=apply)
        return _print(
            {
                "status": "ok",
                "applied": apply,
                "deleted" if apply else "would_delete": counts,
                "noise_retention_days": settings.raw_retention_days,
                "evidence_retention_days": settings.retention_evidence_days,
                "metrics_retention_days": settings.retention_metrics_days,
                "kept_always": "events a person saw or acted on, their stories and copies",
            }
        )

    if command == "sample":
        return await _sample(settings, source=source, limit=min(limit, SAMPLE_MAX), random=random)

    if command == "run":
        from qmemo_radar.interfaces.runtime import run_production

        return await run_production(settings)

    if command == "check-config":
        return _check_config(settings)

    if command == "healthcheck":
        return await _healthcheck(settings)

    app = build_application(settings)

    if command == "init-db":
        await app.repository.initialize()
        print(json.dumps({"status": "ok", "database": str(settings.db_path)}))
        return 0

    if command == "status":
        await app.repository.initialize()
        counts = await app.repository.count_by_status()
        now = datetime.now(UTC)
        print(
            json.dumps(
                {
                    "status": "ok",
                    "database": str(settings.db_path),
                    "events": counts,
                    "outbox_approved": await app.repository.count_packages(OutboxStatus.APPROVED),
                    "ingestion_24h": await app.repository.metrics_since(now - timedelta(days=1)),
                    "cost_month_usd": str(await app.repository.cost_since(month_start(now))),
                    "retention": {
                        "raw_retention_days": settings.raw_retention_days,
                        "evidence_retention_days": settings.retention_evidence_days,
                        "metrics_retention_days": settings.retention_metrics_days,
                        "prunable": "qmemo-radar prune (dry run, counts per category)",
                        "automatic_deletion": False,
                    },
                    "blocked_gaps": {
                        health.source_key: health.blocked_gaps
                        for health in await app.repository.source_health()
                        if health.blocked_gaps
                    },
                    "cost_hard_limit_usd_monthly": str(settings.cost_hard_limit_usd_monthly),
                    "qmemo_publishing": settings.qmemo_publishing_enabled,
                    "x_publishing": settings.x_publishing_enabled,
                },
                ensure_ascii=False,
            )
        )
        return 0

    if command == "dry-run":
        return await _dry_run(settings)

    raise AssertionError(f"Unknown command: {command}")


async def _dry_run(settings: RadarSettings) -> int:
    """Offline path: fixtures -> filter -> rank -> SQLite -> cards -> draft -> outbox."""
    app = build_application(settings)
    await app.repository.initialize()
    gateway = _OfflineGateway()
    services = build_services(
        app,
        collector=FakeCollector(_sample_items()),
        ranker=DeterministicFixtureRanker(),
        writer=DeterministicDraftWriter(),
        gateway=gateway,
        lookup=None,
        sources=SourcesConfig(),
    )
    cycle = await services.runner.run_cycle(manual=True)
    digest_sent = await services.review.deliver(urgent=False)

    draft_id = package_id = None
    if gateway.cards:
        used = await services.drafts.use(gateway.cards[0].event.event_id, DRY_RUN_USER_ID)
        if used.draft is not None:
            draft_id = used.draft.draft_id
            if used.draft.fact_check_status is FactCheckStatus.NEEDS_REVIEW:
                await services.drafts.verify(draft_id, DRY_RUN_USER_ID)
            accepted = await services.drafts.accept(draft_id, DRY_RUN_USER_ID)
            package_id = accepted.package.package_id if accepted.package else None

    print(
        json.dumps(
            {
                "run_status": cycle.status,
                "counters": cycle.counters.model_dump() if cycle.counters else None,
                "cards_sent": {"urgent": cycle.urgent_sent, "digest": digest_sent},
                "draft_id": draft_id,
                "package_id": package_id,
                "outbox_approved": await app.repository.count_packages(OutboxStatus.APPROVED),
                "qmemo_publishing": settings.qmemo_publishing_enabled,
                "x_publishing": settings.x_publishing_enabled,
            },
            ensure_ascii=False,
        )
    )
    return 0


def _check_config(settings: RadarSettings) -> int:
    problems = settings.production_problems()
    summary: dict[str, object] = {"problems": problems}
    if settings.sources_path.is_file():
        try:
            sources = load_sources(settings.sources_path)
        except Exception as exc:
            problems.append(f"sources file is invalid: {str(exc)[:300]}")
        else:
            summary["x_accounts"] = sum(item.enabled for item in sources.x.accounts)
            summary["x_queries"] = sum(item.enabled for item in sources.x.queries)
            summary["sources_enabled"] = enabled_sources(settings, sources)
    summary["paid"] = {
        "sources": settings.paid_sources_enabled,
        "x_search": settings.x_search_enabled,
        "llm_ranking_and_drafts": settings.paid_llm_enabled,
        "cost_target_usd_monthly": str(settings.cost_target_usd_monthly),
        "cost_hard_limit_usd_monthly": str(settings.cost_hard_limit_usd_monthly),
    }
    summary["status"] = "ok" if not problems else "invalid"
    summary["digest_times"] = [moment.strftime("%H:%M") for moment in settings.digest_schedule]
    print(json.dumps(summary, ensure_ascii=False))
    return 0 if not problems else 78


async def _sample(settings: RadarSettings, *, source: str, limit: int, random: bool) -> int:
    """Read-only look at what a source really stored: no network, no LLM, no writes."""
    if not settings.db_path.is_file():
        print(json.dumps({"status": "error", "error": f"database not found: {settings.db_path}"}))
        return 1
    rows = await SQLiteEventRepository(settings.db_path).sample_events(
        source.strip().lower(), limit=limit, random=random
    )
    items = [
        {
            "source": row["source_key"] or row["source"],
            "published_at": row["published_at"],
            "discovered_at": row["discovered_at"],
            "status": row["status"],
            "filter_reason": row["filter_reason"],
            "author": row["author_display_name"] or row["author_handle"],
            "language": row["language"],
            "title": row["title"],
            "text": str(row["original_text"])[:SAMPLE_TEXT_CHARS],
            "url": row["url"],
        }
        for row in rows
    ]
    print(json.dumps({"status": "ok", "count": len(items), "items": items}, ensure_ascii=False))
    return 0


async def _gaps(settings: RadarSettings, *, skip: list[str] | None) -> int:
    """Blocked GDELT minutes: listed, or skipped on explicit request (never automatically).

    ponytail: a collection running at this moment may write its own cursor after the skip; the
    minute then stays listed and the command can simply be repeated.
    """
    if not settings.db_path.is_file():
        print(json.dumps({"status": "error", "error": f"database not found: {settings.db_path}"}))
        return 1
    repository = SQLiteEventRepository(settings.db_path)
    skipped: list[str] = []

    def edit(value: str | None) -> str | None:
        state = GdeltCursor.parse(value)
        wanted = list(state.blocked) if skip == ["all"] else skip or []
        skipped.extend(state.skip(wanted, at=datetime.now(UTC)))
        return state.dump() if value is not None else None

    if skip:
        await repository.initialize()
        await repository.edit_cursor(GDELT_KEY, edit)
    state = GdeltCursor.parse((await repository.get_checkpoints()).get(GDELT_KEY))
    print(
        json.dumps(
            {
                "status": "ok",
                "source_key": GDELT_KEY,
                "next_minute": state.next.strftime("%Y%m%d%H%M%S") if state.next else None,
                "failed_attempts": state.attempts,
                "blocked": state.blocked,
                "skipped_now": skipped,
                "skipped_before": state.skipped,
            },
            ensure_ascii=False,
        )
    )
    return 0


async def _funnel(repository: SQLiteEventRepository, *, hours: int) -> int:
    """From raw items to Telegram cards: flow counters for the period and what is stored now."""
    since = datetime.now(UTC) - timedelta(hours=hours)
    metrics = await repository.metrics_since(since)
    total: Counter[str] = Counter()
    for values in metrics.values():
        total.update(values)
    rejected = {
        name.removeprefix("rejected_"): value
        for name, value in sorted(total.items())
        if name.startswith("rejected_")
    }
    flow = {
        "collected": total["collected"],
        "rejected_before_storage": sum(rejected.values()),
        "rejected_by_reason": rejected,
        "invalid_items": total["invalid_items"],
        "exact_duplicates": total["exact_duplicates"],
        "exact_copies_kept_as_mentions": total["mentions_aggregated"],
        "stored_rows": total["inserted"],
        "filtered_after_storage": total["filtered"],
        "stories_created": total["clusters_created"],
        "near_duplicates": total["near_duplicates"],
        "same_event_texts": total["same_event"],
        "stories_preselected_updates": total["preselected"],
        "stories_reopened": total["reopened"],
        "telegram_delivered": await repository.count_deliveries_since(since),
    }
    return _print(
        {
            "status": "ok",
            "hours": hours,
            "flow": flow,
            "stored_now": await repository.selection_counts(),
            "cost_month_usd": str(await repository.cost_since(month_start(datetime.now(UTC)))),
        }
    )


def _prune_cutoffs(settings: RadarSettings, now: datetime) -> PruneCutoffs:
    window = SelectionConfig().clustering.window_hours
    if settings.sources_path.is_file():
        window = load_sources(settings.sources_path).selection.clustering.window_hours
    return PruneCutoffs(
        noise_before=now - timedelta(days=settings.raw_retention_days),
        evidence_before=now - timedelta(days=settings.retention_evidence_days),
        index_before=now - timedelta(hours=window),
        metrics_before=now - timedelta(days=settings.retention_metrics_days),
    )


def _clip(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    return [
        {**row, "original_text": str(row.get("original_text", ""))[:SAMPLE_TEXT_CHARS]}
        for row in rows
    ]


def _print(payload: dict[str, object], code: int = 0) -> int:
    print(json.dumps(payload, ensure_ascii=False, default=str))
    return code


async def _healthcheck(settings: RadarSettings) -> int:
    """Healthy when the scheduler heartbeat in SQLite is recent. Never creates the database."""
    heartbeat = None
    if settings.db_path.is_file():
        heartbeat = await SQLiteEventRepository(settings.db_path).get_state(HEARTBEAT_KEY)
    age = datetime.now(UTC) - datetime.fromisoformat(heartbeat) if heartbeat else None
    healthy = age is not None and age < HEARTBEAT_MAX_AGE
    # `is not None`: a zero timedelta is falsy, and the heartbeat may be written this instant.
    seconds = age.total_seconds() if age is not None else None
    print(json.dumps({"healthy": healthy, "heartbeat_age_seconds": seconds}))
    return 0 if healthy else 1


class _OfflineGateway:
    """Dry-run stand-in for Telegram: keeps cards in memory and sends nothing."""

    _message_ids = itertools.count(time.time_ns() // 1_000_000)

    def __init__(self) -> None:
        self.cards: list[ScoredEvent] = []

    async def send_card(self, card: ScoredEvent, *, urgent: bool) -> int:
        self.cards.append(card)
        return next(self._message_ids)


def _sample_items() -> list[RawSourceItem]:
    now = datetime.now(UTC)
    return [
        RawSourceItem(
            source=SourceType.X,
            external_id="dry-run-1",
            url=HttpUrl("https://x.com/example/status/1?utm_source=test"),
            author_handle="example",
            author_display_name="Example Founder",
            original_text='Founder said: "Predictions should be remembered, not rewritten."',
            language="en",
            published_at=now,
            engagement=Engagement(likes=120, reposts=15, replies=40, quotes=8),
            source_key="dry-run",
        ),
        RawSourceItem(
            source=SourceType.X,
            external_id="dry-run-2",
            url=HttpUrl("https://x.com/example/status/2"),
            author_handle="example",
            original_text="A long thread about the roadmap for the next product release cycle.",
            language="en",
            published_at=now - timedelta(minutes=10),
            source_key="dry-run",
        ),
        RawSourceItem(
            source=SourceType.X,
            external_id="dry-run-3",
            url=HttpUrl("https://x.com/example/status/3"),
            author_handle="example",
            original_text='Founder said: "This old statement is outside the collection window."',
            language="en",
            published_at=now - timedelta(hours=3),
            source_key="dry-run",
        ),
    ]


def main() -> None:
    args = build_parser().parse_args()
    # JSON output carries text from any language; a Windows console or pipe defaults to cp1252.
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[union-attr]
    raise SystemExit(
        asyncio.run(
            execute(
                args.command,
                db_path=args.db,
                source=args.source,
                limit=args.limit,
                random=args.random,
                skip=args.skip,
                target=args.target,
                hours=args.hours,
                state=args.state,
                order=args.order,
                reason=args.reason,
                apply=args.apply,
            )
        )
    )

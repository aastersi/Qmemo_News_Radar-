"""Composition root: the only place where concrete adapters are chosen and connected."""

import json
import logging
import re
import sys
from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import httpx

from qmemo_radar.application.budget import BudgetGuard, PaidFeature
from qmemo_radar.application.collection import MultiSourceCollector
from qmemo_radar.application.drafting import DraftService
from qmemo_radar.application.filtering import FilterPolicy
from qmemo_radar.application.free_ranking import FreeRanker
from qmemo_radar.application.normalization import comparison_text
from qmemo_radar.application.pipeline import PipelineThresholds, RadarPipeline
from qmemo_radar.application.ports import (
    DraftWriter,
    PostLookup,
    QuotePublisher,
    Ranker,
    ReviewGateway,
    SourceCollector,
    XPublisher,
)
from qmemo_radar.application.review import DeliveryLimits, ReviewService
from qmemo_radar.application.runner import RadarRunner
from qmemo_radar.application.scheduler import RadarScheduler
from qmemo_radar.application.selection import SelectionPolicy, topic
from qmemo_radar.config import RadarSettings, SelectionConfig, SourcesConfig
from qmemo_radar.exceptions import ProductionAdapterNotConfigured
from qmemo_radar.infrastructure.collectors.gdelt_gqg import GdeltQuotationCollector
from qmemo_radar.infrastructure.collectors.rss import Feed, RssCollector
from qmemo_radar.infrastructure.collectors.x_api import (
    X_API_BASE_URL,
    XApiClient,
    XQuery,
    XRecentSearchCollector,
)
from qmemo_radar.infrastructure.drafting import DisabledDraftWriter, LlmDraftWriter
from qmemo_radar.infrastructure.llm import ChatCompletionsClient
from qmemo_radar.infrastructure.publishing import DisabledQuotePublisher, DisabledXPublisher
from qmemo_radar.infrastructure.ranking import LlmRanker
from qmemo_radar.infrastructure.storage import SQLiteEventRepository

if TYPE_CHECKING:
    from aiogram import Bot, Dispatcher

    from qmemo_radar.interfaces.telegram.controller import TelegramController

logger = logging.getLogger(__name__)

_LOG_FIELDS = ("run_id", "event_id", "source_key", "operation", "result", "error_code")


@dataclass(frozen=True, slots=True)
class Application:
    settings: RadarSettings
    repository: SQLiteEventRepository


@dataclass(frozen=True, slots=True)
class Services:
    pipeline: RadarPipeline
    review: ReviewService
    drafts: DraftService
    runner: RadarRunner


@dataclass(frozen=True, slots=True)
class Runtime:
    repository: SQLiteEventRepository
    services: Services
    scheduler: RadarScheduler
    controller: "TelegramController"
    bot: "Bot"
    dispatcher: "Dispatcher"
    quote_publisher: QuotePublisher
    x_publisher: XPublisher


def build_application(settings: RadarSettings | None = None) -> Application:
    resolved = settings or RadarSettings()
    resolved.ensure_data_directory()
    return Application(
        settings=resolved,
        repository=SQLiteEventRepository(resolved.db_path),
    )


def build_pipeline(
    application: Application,
    *,
    collector: SourceCollector,
    ranker: Ranker | None,
    sources: SourcesConfig | None = None,
) -> RadarPipeline:
    settings = application.settings
    resolved_sources = sources or SourcesConfig()
    return RadarPipeline(
        collector=collector,
        ranker=ranker,
        repository=application.repository,
        filter_policy=FilterPolicy(
            max_age=timedelta(minutes=settings.max_event_age_minutes),
            blocked_authors=frozenset(
                item.removeprefix("@").casefold() for item in resolved_sources.blocked_authors
            ),
            blocked_terms=frozenset(
                comparison_text(item) for item in resolved_sources.blocked_terms
            ),
        ),
        thresholds=PipelineThresholds(
            archive=settings.archive_threshold,
            digest=settings.digest_threshold,
            urgent=settings.urgent_threshold,
        ),
        selection=selection_policy(resolved_sources.selection),
    )


def selection_policy(config: SelectionConfig) -> SelectionPolicy:
    """sources.yaml `selection` as the rules the pipeline applies (patterns compiled once)."""
    gate, clustering, preselection = config.gate, config.clustering, config.preselection
    return SelectionPolicy(
        min_words=gate.min_words,
        min_chars=gate.min_chars,
        max_chars=gate.max_chars,
        languages=frozenset(language.strip().casefold() for language in gate.languages),
        blocked_terms=tuple(comparison_text(term) for term in gate.blocked_terms),
        blocked_domains=tuple(
            domain.strip().lower().removeprefix("www.") for domain in gate.blocked_domains
        ),
        templates=tuple(re.compile(pattern) for pattern in gate.template_patterns),
        min_letter_ratio=gate.min_letter_ratio,
        max_url_age=timedelta(days=gate.max_url_age_days) if gate.max_url_age_days else None,
        require_topic=gate.require_topic,
        topics=tuple(topic(rule.name, rule.terms, rule.weight) for rule in config.topics),
        window=timedelta(hours=clustering.window_hours),
        near_duplicate_jaccard=clustering.near_duplicate_jaccard,
        same_event_overlap=clustering.same_event_overlap,
        same_event_min_shared=clustering.same_event_min_shared,
        min_preselect_score=preselection.min_score,
        always_rank_sources=frozenset(preselection.always_rank_sources),
        boilerplate_min_mentions=preselection.boilerplate_min_mentions,
        boilerplate_max_domains=preselection.boilerplate_max_domains,
    )


def build_services(
    application: Application,
    *,
    collector: SourceCollector,
    ranker: Ranker | None,
    writer: DraftWriter,
    gateway: ReviewGateway,
    lookup: PostLookup | None,
    sources: SourcesConfig,
) -> Services:
    settings = application.settings
    repository = application.repository
    pipeline = build_pipeline(application, collector=collector, ranker=ranker, sources=sources)
    review = ReviewService(
        repository=repository,
        gateway=gateway,
        chat_id=settings.allowed_telegram_id or 0,
        limits=DeliveryLimits(
            daily_cards=settings.daily_card_limit,
            digest_cards=settings.digest_card_limit,
            urgent_threshold=settings.urgent_threshold,
            event_ttl=timedelta(hours=settings.event_ttl_hours),
        ),
        timezone=settings.zone,
        lookup=lookup,
    )
    return Services(
        pipeline=pipeline,
        review=review,
        drafts=DraftService(repository=repository, writer=writer),
        runner=RadarRunner(
            pipeline=pipeline,
            repository=repository,
            review=review,
            timezone=settings.zone,
            qmemo_publishing_enabled=settings.qmemo_publishing_enabled,
            x_publishing_enabled=settings.x_publishing_enabled,
        ),
    )


def build_publishers(settings: RadarSettings) -> tuple[QuotePublisher, XPublisher]:
    """Only the disabled publishers exist. Enabling a flag fails closed instead of publishing."""
    if settings.qmemo_publishing_enabled or settings.x_publishing_enabled:
        raise ProductionAdapterNotConfigured("No real Quote Memorial or X publisher is registered")
    return DisabledQuotePublisher(), DisabledXPublisher()


@dataclass(frozen=True, slots=True)
class SourceContext:
    """What collector factories may use. Paid clients are None unless their feature is on."""

    settings: RadarSettings
    sources: SourcesConfig
    x_client: XApiClient | None
    # Shared client of the free sources (GDELT, RSS): never authenticated, never redirected.
    free_http: httpx.AsyncClient | None = None


@dataclass(frozen=True, slots=True)
class SourceRegistration:
    name: str
    enabled: Callable[[RadarSettings, SourcesConfig], bool]
    build: Callable[[SourceContext], SourceCollector]


def _x_search_enabled(settings: RadarSettings, sources: SourcesConfig) -> bool:
    configured = any(item.enabled for item in sources.x.accounts) or any(
        item.enabled for item in sources.x.queries
    )
    return settings.x_search_enabled and configured


def _build_x_search(context: SourceContext) -> SourceCollector:
    if context.x_client is None:
        raise ValueError("RADAR_X_BEARER_TOKEN is required for X search")
    return build_x_collector(context.x_client, context.settings, context.sources)


def _free_http(context: SourceContext) -> httpx.AsyncClient:
    if context.free_http is None:
        raise ValueError("free sources need the shared HTTP client")
    return context.free_http


def _build_gdelt(context: SourceContext) -> SourceCollector:
    settings = context.settings
    return GdeltQuotationCollector(
        _free_http(context),
        safety_lag=timedelta(minutes=settings.gdelt_safety_lag_minutes),
        max_minutes_per_run=settings.gdelt_max_minutes_per_run,
        first_run_lookback=timedelta(minutes=settings.max_event_age_minutes),
        languages=settings.gdelt_language_set,
        allow_unknown_language=settings.gdelt_allow_unknown_language,
        block_after_failures=settings.gdelt_block_after_failures,
    )


def _rss_enabled(_: RadarSettings, sources: SourcesConfig) -> bool:
    return any(feed.enabled for feed in sources.rss.feeds)


def _build_rss(context: SourceContext) -> SourceCollector:
    feeds = [Feed(item.name, str(item.url)) for item in context.sources.rss.feeds if item.enabled]
    return RssCollector(
        _free_http(context), feeds, max_bytes=context.settings.rss_max_response_bytes
    )


# One entry per source type. A disabled entry is never built, so it needs no credentials.
SOURCE_REGISTRY: tuple[SourceRegistration, ...] = (
    SourceRegistration("x_search", _x_search_enabled, _build_x_search),
    SourceRegistration("gdelt_gqg", lambda settings, _: settings.gdelt_enabled, _build_gdelt),
    SourceRegistration("rss", _rss_enabled, _build_rss),
)


def enabled_sources(
    settings: RadarSettings,
    sources: SourcesConfig,
    registry: Sequence[SourceRegistration] = SOURCE_REGISTRY,
) -> list[str]:
    return [entry.name for entry in registry if entry.enabled(settings, sources)]


def build_collector(
    context: SourceContext,
    registry: Sequence[SourceRegistration] = SOURCE_REGISTRY,
) -> MultiSourceCollector:
    return MultiSourceCollector(
        {
            entry.name: entry.build(context)
            for entry in registry
            if entry.enabled(context.settings, context.sources)
        }
    )


@asynccontextmanager
async def build_runtime(settings: RadarSettings, sources: SourcesConfig) -> AsyncIterator[Runtime]:
    """Wire production adapters and close every HTTP client and the bot session on exit.

    Paid clients are created only for enabled paid features, and all of them share one guard.
    """
    from qmemo_radar.interfaces.telegram.bot import (
        TelegramReviewGateway,
        build_bot,
        build_dispatcher,
    )
    from qmemo_radar.interfaces.telegram.controller import TelegramController

    if settings.telegram_bot_token is None or settings.allowed_telegram_id is None:
        raise ValueError("RADAR_TELEGRAM_BOT_TOKEN and RADAR_ALLOWED_TELEGRAM_ID are required")
    quote_publisher, x_publisher = build_publishers(settings)
    application = build_application(settings)
    guard = build_budget_guard(settings, application.repository)
    async with AsyncExitStack() as stack:
        x_client = None
        if settings.paid_sources_enabled and settings.x_bearer_token is not None:
            x_http = await stack.enter_async_context(build_x_http_client(settings))
            x_client = XApiClient(x_http, guard=guard)
        llm = None
        if settings.paid_llm_enabled:
            llm_http = await stack.enter_async_context(build_llm_http_client(settings))
            llm = build_llm_client(settings, llm_http, guard)
        bot = build_bot(settings.telegram_bot_token.get_secret_value())
        stack.push_async_callback(bot.session.close)
        free_http = await stack.enter_async_context(build_free_http_client())

        collector = build_collector(SourceContext(settings, sources, x_client, free_http))
        # An upgrade from the X pilot without the new paid flags would otherwise run silently idle.
        logger.log(
            logging.INFO if collector.names else logging.WARNING,
            "radar capabilities",
            extra={
                "operation": "startup",
                "result": f"sources={','.join(collector.names) or 'none'} "
                f"ranking={'llm' if llm else 'free'} x_lookup={'on' if x_client else 'off'}",
            },
        )
        # Without a paid LLM the free ranker scores preselected stories: $0 and no network.
        free = FreeRanker(application.repository, selection_policy(sources.selection))
        services = build_services(
            application,
            collector=collector,
            ranker=LlmRanker(llm) if llm else free,
            writer=LlmDraftWriter(llm) if llm else DisabledDraftWriter(),
            gateway=TelegramReviewGateway(
                bot, chat_id=settings.allowed_telegram_id, timezone=settings.zone
            ),
            lookup=x_client,
            sources=sources,
        )
        controller = TelegramController(
            allowed_user_id=settings.allowed_telegram_id,
            review=services.review,
            drafts=services.drafts,
            runner=services.runner,
            timezone=settings.zone,
        )
        yield Runtime(
            repository=application.repository,
            services=services,
            scheduler=RadarScheduler(
                runner=services.runner,
                review=services.review,
                collect_every=timedelta(minutes=settings.collect_interval_minutes),
                digest_times=settings.digest_schedule,
                timezone=settings.zone,
            ),
            controller=controller,
            bot=bot,
            dispatcher=build_dispatcher(controller),
            quote_publisher=quote_publisher,
            x_publisher=x_publisher,
        )


def build_free_http_client() -> httpx.AsyncClient:
    """For keyless public sources. Redirects are followed by the collector, one checked hop at
    a time, so a feed cannot bounce the request to an internal address."""
    return httpx.AsyncClient(
        headers={"User-Agent": "qmemo-news-radar/0.1"},
        timeout=httpx.Timeout(30.0, connect=10.0),
        follow_redirects=False,
    )


def build_x_http_client(settings: RadarSettings) -> httpx.AsyncClient:
    if settings.x_bearer_token is None:
        raise ValueError("RADAR_X_BEARER_TOKEN is required")
    return httpx.AsyncClient(
        base_url=X_API_BASE_URL,
        headers={"Authorization": f"Bearer {settings.x_bearer_token.get_secret_value()}"},
        timeout=httpx.Timeout(20.0, connect=10.0),
    )


def build_x_collector(
    client: XApiClient,
    settings: RadarSettings,
    sources: SourcesConfig,
) -> XRecentSearchCollector:
    queries = [
        XQuery(f"account:{account.handle.casefold()}", f"from:{account.handle} -is:retweet")
        for account in sources.x.accounts
        if account.enabled
    ] + [XQuery(f"query:{query.name}", query.query) for query in sources.x.queries if query.enabled]
    return XRecentSearchCollector(
        client,
        queries,
        lookback=timedelta(minutes=settings.max_event_age_minutes),
        max_pages=sources.x.max_pages_per_query,
    )


def build_budget_guard(settings: RadarSettings, ledger: SQLiteEventRepository) -> BudgetGuard:
    """The single BudgetGuard of a process. Every paid adapter must receive this instance."""
    enabled = {
        PaidFeature.X_SEARCH: settings.x_search_enabled,
        PaidFeature.X_LOOKUP: settings.paid_sources_enabled,
        PaidFeature.LLM: settings.paid_llm_enabled,
    }
    return BudgetGuard(
        ledger,
        enabled=frozenset(feature for feature, on in enabled.items() if on),
        hard_limit_usd=settings.cost_hard_limit_usd_monthly,
        target_usd=settings.cost_target_usd_monthly,
    )


def build_llm_client(
    settings: RadarSettings, http: httpx.AsyncClient, guard: BudgetGuard
) -> ChatCompletionsClient:
    if not settings.llm_model:
        raise ValueError("RADAR_LLM_MODEL is required")
    return ChatCompletionsClient(
        http,
        model=settings.llm_model,
        guard=guard,
        cost_per_call_usd=settings.llm_cost_per_call_usd,
        temperature=settings.llm_temperature,
    )


def build_llm_http_client(settings: RadarSettings) -> httpx.AsyncClient:
    if not settings.llm_base_url or settings.llm_api_key is None:
        raise ValueError("RADAR_LLM_BASE_URL and RADAR_LLM_API_KEY are required")
    return httpx.AsyncClient(
        base_url=settings.llm_base_url,
        headers={"Authorization": f"Bearer {settings.llm_api_key.get_secret_value()}"},
        timeout=httpx.Timeout(120.0, connect=10.0),
    )


class JsonLogFormatter(logging.Formatter):
    """One JSON object per line with the structured fields; known secrets are redacted."""

    def __init__(self, secrets: Sequence[str] = ()) -> None:
        super().__init__()
        self._secrets = [secret for secret in secrets if secret]

    def format(self, record: logging.LogRecord) -> str:
        entry: dict[str, object] = {
            "time": datetime.fromtimestamp(record.created, UTC).isoformat(),
            "level": record.levelname,
            "module": record.name,
            "message": record.getMessage(),
        }
        for field in _LOG_FIELDS:
            value = getattr(record, field, None)
            if value is not None:
                entry[field] = value
        if record.exc_info:
            entry["exception"] = self.formatException(record.exc_info)
        line = json.dumps(entry, ensure_ascii=False, default=str)
        for secret in self._secrets:
            line = line.replace(secret, "[redacted]")
        return line


def configure_logging(settings: RadarSettings) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonLogFormatter(settings.secret_values()))
    logging.basicConfig(level=settings.log_level.upper(), handlers=[handler], force=True)
    # These libraries log SQL parameters, request URLs or every update at low levels.
    for noisy in ("aiosqlite", "httpx", "httpcore", "aiogram.event"):
        logging.getLogger(noisy).setLevel(logging.WARNING)

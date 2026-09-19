from datetime import time
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import yaml
from pydantic import BaseModel, ConfigDict, Field, HttpUrl, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class RadarSettings(BaseSettings):
    """Runtime configuration loaded only at the composition root."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="RADAR_",
        extra="ignore",
        case_sensitive=False,
        # `RADAR_ALLOWED_TELEGRAM_ID=` copied from .env.example means "not set", not an error.
        env_ignore_empty=True,
    )

    environment: str = "development"
    log_level: str = "INFO"
    timezone: str = "Asia/Ho_Chi_Minh"
    db_path: Path = Path("data/radar.db")
    sources_path: Path = Path("sources.yaml")

    telegram_bot_token: SecretStr | None = None
    allowed_telegram_id: int | None = None

    x_bearer_token: SecretStr | None = None
    llm_base_url: str | None = None
    llm_api_key: SecretStr | None = None
    llm_model: str | None = None
    llm_temperature: float = Field(default=0.0, ge=0.0, le=1.0)

    collect_interval_minutes: int = Field(default=30, ge=5, le=1440)
    max_event_age_minutes: int = Field(default=60, ge=5, le=10080)
    daily_card_limit: int = Field(default=10, ge=1, le=50)
    digest_card_limit: int = Field(default=5, ge=1, le=5)
    digest_times: str = "10:00,15:00,20:00"
    urgent_threshold: int = Field(default=80, ge=0, le=100)
    digest_threshold: int = Field(default=65, ge=0, le=100)
    archive_threshold: int = Field(default=50, ge=0, le=100)
    event_ttl_hours: int = Field(default=48, ge=1, le=720)

    qmemo_publishing_enabled: bool = False
    x_publishing_enabled: bool = False

    # External API money. The hard limit can be lowered but never raised above $10/month.
    cost_target_usd_monthly: Decimal = Field(default=Decimal(0), ge=0, le=10)
    cost_hard_limit_usd_monthly: Decimal = Field(default=Decimal(10), ge=0, le=10)
    paid_sources_enabled: bool = False
    paid_llm_enabled: bool = False
    x_paid_search_enabled: bool = False
    # The price of one LLM request is unknown to the code; without this estimate every paid
    # LLM call is blocked. Use 0 only for a provider that is really free (e.g. a local model).
    llm_cost_per_call_usd: Decimal | None = Field(default=None, ge=0, le=1)

    # Noise (FILTERED_OUT, EXPIRED, ARCHIVED without any human action) older than this is
    # reported as prunable. Nothing is deleted automatically yet.
    raw_retention_days: int = Field(default=14, ge=7, le=365)

    # Free sources: no key, no BudgetGuard. GDELT is off by default because it adds tens of
    # thousands of quotes per hour; RSS is on as soon as sources.yaml lists an enabled feed.
    gdelt_enabled: bool = False
    # Minutes newer than now minus this are never requested: a 404 there may be a late file.
    gdelt_safety_lag_minutes: int = Field(default=10, ge=2, le=1440)
    # Minute files checked per collection; bounds one catch-up run after a long downtime and the
    # items held in memory (measured: up to ~33k English quotes, ~100 MB, per hour of files).
    gdelt_max_minutes_per_run: int = Field(default=60, ge=1, le=120)
    # Comma-separated GDELT language names (e.g. English,Spanish), case-insensitive; * = all.
    gdelt_languages: str = "English"
    gdelt_allow_unknown_language: bool = False
    # A minute failing (403, 5xx, network) in this many runs in a row is parked as a blocked gap,
    # retried once per run and listed by `qmemo-radar gaps`; collection moves on meanwhile.
    gdelt_block_after_failures: int = Field(default=3, ge=2, le=100)
    rss_max_response_bytes: int = Field(default=5_000_000, ge=10_000, le=5_000_000)

    @model_validator(mode="after")
    def validate_thresholds_and_timezone(self) -> "RadarSettings":
        if not (self.archive_threshold <= self.digest_threshold <= self.urgent_threshold):
            raise ValueError("Thresholds must satisfy archive <= digest <= urgent")
        if self.gdelt_enabled and self.gdelt_max_minutes_per_run < self.collect_interval_minutes:
            raise ValueError(
                "RADAR_GDELT_MAX_MINUTES_PER_RUN must be at least RADAR_COLLECT_INTERVAL_MINUTES,"
                " otherwise GDELT falls further behind on every run"
            )
        if self.cost_target_usd_monthly > self.cost_hard_limit_usd_monthly:
            raise ValueError("RADAR_COST_TARGET_USD_MONTHLY must not exceed the hard limit")
        try:
            ZoneInfo(self.timezone)
        except ZoneInfoNotFoundError as exc:
            raise ValueError(f"Unknown timezone: {self.timezone}") from exc
        self.digest_schedule  # noqa: B018 - validates RADAR_DIGEST_TIMES early
        return self

    @property
    def zone(self) -> ZoneInfo:
        return ZoneInfo(self.timezone)

    @property
    def digest_schedule(self) -> tuple[time, ...]:
        try:
            times = tuple(
                sorted({time.fromisoformat(v.strip()) for v in self.digest_times.split(",")})
            )
        except ValueError as exc:
            raise ValueError("RADAR_DIGEST_TIMES must look like 10:00,15:00,20:00") from exc
        if not 2 <= len(times) <= 3:
            raise ValueError("RADAR_DIGEST_TIMES must contain two or three different times")
        return times

    @property
    def gdelt_language_set(self) -> frozenset[str] | None:
        """Casefolded allowed languages; None means every language."""
        names = {name.strip().casefold() for name in self.gdelt_languages.split(",")}
        names.discard("")
        return None if "*" in names else frozenset(names)

    @property
    def x_search_enabled(self) -> bool:
        return self.paid_sources_enabled and self.x_paid_search_enabled

    def production_problems(self) -> list[str]:
        """What prevents `qmemo-radar run`. Empty means the configuration is complete.

        Credentials are required only for what is enabled: a disabled paid feature needs nothing.
        """
        required: dict[str, object] = {
            "RADAR_TELEGRAM_BOT_TOKEN": self.telegram_bot_token,
            "RADAR_ALLOWED_TELEGRAM_ID": self.allowed_telegram_id,
        }
        if self.x_search_enabled:
            required["RADAR_X_BEARER_TOKEN"] = self.x_bearer_token
        if self.paid_llm_enabled:
            required |= {
                "RADAR_LLM_BASE_URL": self.llm_base_url,
                "RADAR_LLM_API_KEY": self.llm_api_key,
                "RADAR_LLM_MODEL": self.llm_model,
                "RADAR_LLM_COST_PER_CALL_USD": self.llm_cost_per_call_usd,
            }
        # `in (None, "")`, not `not value`: a cost estimate of 0 is a valid setting.
        problems = [f"{name} is required" for name, val in required.items() if val in (None, "")]
        if self.x_paid_search_enabled and not self.paid_sources_enabled:
            problems.append("RADAR_X_PAID_SEARCH_ENABLED=true requires RADAR_PAID_SOURCES_ENABLED")
        # No real publisher exists yet, so enabling publishing must stop the service.
        if self.qmemo_publishing_enabled:
            problems.append("RADAR_QMEMO_PUBLISHING_ENABLED must stay false in this version")
        if self.x_publishing_enabled:
            problems.append("RADAR_X_PUBLISHING_ENABLED must stay false in this version")
        if not self.sources_path.is_file():
            problems.append(f"sources file not found: {self.sources_path}")
        return problems

    def secret_values(self) -> list[str]:
        secrets = (self.telegram_bot_token, self.x_bearer_token, self.llm_api_key)
        return [secret.get_secret_value() for secret in secrets if secret]

    def ensure_data_directory(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)


class _SourcesModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class XAccountSource(_SourcesModel):
    handle: str = Field(pattern=r"^[A-Za-z0-9_]{1,15}$")
    enabled: bool = True


class XQuerySource(_SourcesModel):
    name: str = Field(pattern=r"^[a-z0-9_]{1,40}$")
    # Self-serve X API access allows recent-search queries of up to 512 characters.
    query: str = Field(min_length=1, max_length=512)
    enabled: bool = True


class XSources(_SourcesModel):
    accounts: tuple[XAccountSource, ...] = ()
    queries: tuple[XQuerySource, ...] = ()
    # Each page can return up to 100 billed post reads, so one page is the safe default.
    max_pages_per_query: int = Field(default=1, ge=1, le=10)


class RssFeed(_SourcesModel):
    name: str = Field(pattern=r"^[a-z0-9_]{1,40}$")
    url: HttpUrl
    enabled: bool = True


class RssSources(_SourcesModel):
    feeds: tuple[RssFeed, ...] = ()


class SourcesConfig(_SourcesModel):
    """Contents of sources.yaml: what to read and what to always drop."""

    x: XSources = XSources()
    rss: RssSources = RssSources()
    blocked_authors: tuple[str, ...] = ()
    blocked_terms: tuple[str, ...] = ()

    @model_validator(mode="after")
    def validate_unique_keys(self) -> "SourcesConfig":
        handles = [account.handle.casefold() for account in self.x.accounts]
        names = [query.name for query in self.x.queries]
        if len(handles) != len(set(handles)) or len(names) != len(set(names)):
            raise ValueError("Account handles and query names in sources.yaml must be unique")
        feeds = [feed.name for feed in self.rss.feeds]
        if len(feeds) != len(set(feeds)):
            raise ValueError("RSS feed names in sources.yaml must be unique")
        return self


def load_sources(path: Path) -> SourcesConfig:
    return SourcesConfig.model_validate(yaml.safe_load(path.read_text(encoding="utf-8")) or {})

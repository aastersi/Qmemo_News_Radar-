from collections.abc import Callable, Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import pytest
from fakes import open_guard, snowflake

from qmemo_radar.application.filtering import FilterPolicy
from qmemo_radar.application.normalization import parse_x_status_url
from qmemo_radar.application.pipeline import PipelineThresholds, RadarPipeline
from qmemo_radar.domain import EventCandidate
from qmemo_radar.exceptions import SourceUnavailable
from qmemo_radar.infrastructure.collectors.x_api import XApiClient, XQuery, XRecentSearchCollector
from qmemo_radar.infrastructure.ranking import DeterministicFixtureRanker
from qmemo_radar.infrastructure.storage import SQLiteEventRepository

Handler = Callable[[httpx.Request], httpx.Response]


def post(post_id: str, text: str, *, author_id: str = "u1", minutes_ago: int = 5) -> dict[str, Any]:
    created = datetime.now(UTC) - timedelta(minutes=minutes_ago)
    return {
        "id": post_id,
        "text": text,
        "author_id": author_id,
        "created_at": created.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
        "lang": "en",
        "public_metrics": {
            "retweet_count": 3,
            "reply_count": 7,
            "like_count": 42,
            "quote_count": 1,
            "impression_count": 900,
        },
    }


def page(posts: list[dict[str, Any]], *, next_token: str | None = None) -> dict[str, Any]:
    meta: dict[str, Any] = {"result_count": len(posts)}
    if posts:
        meta["newest_id"] = posts[0]["id"]
        meta["oldest_id"] = posts[-1]["id"]
    if next_token:
        meta["next_token"] = next_token
    return {
        "data": posts,
        "includes": {"users": [{"id": "u1", "username": "founder", "name": "Founder Name"}]},
        "meta": meta,
    }


class Recorder:
    def __init__(self, *responses: httpx.Response | Handler) -> None:
        self.requests: list[httpx.Request] = []
        self._responses = list(responses)
        self.sleeps: list[float] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        response = self._responses.pop(0) if len(self._responses) > 1 else self._responses[0]
        return response(request) if callable(response) else response

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)

    def client(self) -> XApiClient:
        http = httpx.AsyncClient(
            base_url="https://api.x.com", transport=httpx.MockTransport(self.handler)
        )
        return XApiClient(http, guard=open_guard(), sleep=self.sleep)


def collector(recorder: Recorder, *queries: str, pages: int = 1) -> XRecentSearchCollector:
    return XRecentSearchCollector(
        recorder.client(),
        [XQuery(f"query:{name}", f"{name} -is:retweet") for name in queries or ("main",)],
        lookback=timedelta(minutes=60),
        max_pages=pages,
    )


def pipeline(source: XRecentSearchCollector, repo: SQLiteEventRepository) -> RadarPipeline:
    return RadarPipeline(
        collector=source,
        ranker=DeterministicFixtureRanker(),
        repository=repo,
        filter_policy=FilterPolicy(max_age=timedelta(hours=1)),
        thresholds=PipelineThresholds(),
    )


async def test_successful_search_parses_posts_and_requests_only_used_fields() -> None:
    long_post = post("200", "Short &amp; truncated")
    long_post["note_tweet"] = {"text": "Full long text &amp; more that was cut at 280 characters"}
    recorder = Recorder(httpx.Response(200, json=page([long_post])))

    fetches = await collector(recorder).collect({})

    params = recorder.requests[0].url.params
    assert recorder.requests[0].url.path == "/2/tweets/search/recent"
    assert params["tweet.fields"] == "author_id,created_at,lang,note_tweet,public_metrics"
    assert params["expansions"] == "author_id"
    assert params["user.fields"] == "name,username"
    assert "start_time" in params and "since_id" not in params
    [fetch] = fetches
    assert fetch.error_code is None and fetch.cursor == "200"
    [item] = fetch.items
    assert item.original_text == "Full long text & more that was cut at 280 characters"
    assert item.author_handle == "founder" and item.author_display_name == "Founder Name"
    assert str(item.url) == "https://x.com/founder/status/200"
    assert (item.engagement.likes, item.engagement.reposts, item.engagement.replies) == (42, 3, 7)
    assert item.engagement.views == 900
    assert item.source_key == "query:main"


async def test_pagination_follows_next_token_and_keeps_newest_id() -> None:
    since = snowflake(minutes_ago=30)
    recorder = Recorder(
        httpx.Response(
            200, json=page([post("30", "newest post text"), post("29", "b")], next_token="t2")
        ),
        httpx.Response(200, json=page([post("28", "older post text")])),
    )

    [fetch] = await collector(recorder, pages=3).collect({"query:main": since})

    assert [item.external_id for item in fetch.items] == ["30", "29", "28"]
    assert fetch.cursor == "30"
    assert "next_token" not in recorder.requests[0].url.params
    assert recorder.requests[1].url.params["next_token"] == "t2"
    assert recorder.requests[1].url.params["since_id"] == since


async def test_since_id_replaces_start_time_and_empty_result_keeps_cursor() -> None:
    since = snowflake(minutes_ago=90)
    recorder = Recorder(httpx.Response(200, json={"meta": {"result_count": 0}}))

    [fetch] = await collector(recorder).collect({"query:main": since})

    params = recorder.requests[0].url.params
    assert params["since_id"] == since
    assert "start_time" not in params
    assert fetch.items == () and fetch.cursor == since


async def test_since_id_older_than_the_search_window_falls_back_to_start_time() -> None:
    stale = snowflake(minutes_ago=7 * 24 * 60)
    recorder = Recorder(httpx.Response(200, json={"meta": {"result_count": 0}}))

    await collector(recorder).collect({"query:main": stale})

    params = recorder.requests[0].url.params
    assert "since_id" not in params and "start_time" in params


async def test_truncated_pages_are_reported(caplog: pytest.LogCaptureFixture) -> None:
    body = page([post("9", "more pages exist")], next_token="t")
    recorder = Recorder(httpx.Response(200, json=body))

    [fetch] = await collector(recorder, pages=1).collect({})

    assert len(recorder.requests) == 1 and fetch.cursor == "9"
    assert any(getattr(r, "result", None) == "truncated" for r in caplog.records)


async def test_malformed_shapes_do_not_break_the_source() -> None:
    bad_note = post("8", "note is not an object")
    bad_note["note_tweet"] = "oops"
    body = {"data": [bad_note, post("7", "fine post text here")], "includes": [], "meta": "x"}
    recorder = Recorder(httpx.Response(200, json=body))

    [fetch] = await collector(recorder).collect({})

    assert fetch.error_code is None
    assert [item.external_id for item in fetch.items] == ["7"]


async def test_429_waits_for_retry_after_then_succeeds() -> None:
    recorder = Recorder(
        httpx.Response(429, headers={"retry-after": "7"}),
        httpx.Response(200, json=page([post("1", "text after rate limit")])),
    )

    [fetch] = await collector(recorder).collect({})

    assert recorder.sleeps == [7.0]
    assert len(fetch.items) == 1


async def test_429_with_long_reset_fails_the_source_without_sleeping() -> None:
    reset = str(int(datetime.now(UTC).timestamp()) + 900)
    recorder = Recorder(httpx.Response(429, headers={"x-rate-limit-reset": reset}))

    [fetch] = await collector(recorder).collect({})

    assert fetch.error_code == "rate_limited_429"
    assert recorder.sleeps == [] and len(recorder.requests) == 1


async def test_5xx_is_retried_at_most_three_times() -> None:
    recovering = Recorder(
        httpx.Response(503),
        httpx.Response(502),
        httpx.Response(200, json=page([post("1", "recovered after server errors")])),
    )
    failing = Recorder(httpx.Response(500))

    [recovered] = await collector(recovering).collect({})
    [failed] = await collector(failing).collect({})

    assert len(recovering.requests) == 3 and len(recovered.items) == 1
    assert len(failing.requests) == 3 and failed.error_code == "server_error_500"


async def test_network_error_is_retried() -> None:
    def broken(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectTimeout("timeout", request=request)

    recorder = Recorder(broken)

    [fetch] = await collector(recorder).collect({})

    assert len(recorder.requests) == 3 and fetch.error_code == "network_error"


async def test_other_4xx_is_not_retried() -> None:
    recorder = Recorder(httpx.Response(401, json={"title": "Unauthorized"}))

    [fetch] = await collector(recorder).collect({})

    assert len(recorder.requests) == 1
    assert fetch.error_code == "client_error_401"
    assert recorder.sleeps == []


def test_manual_link_accepts_only_x_status_urls() -> None:
    assert parse_x_status_url("https://x.com/founder/status/1234567890") == "1234567890"
    assert parse_x_status_url(" https://x.com/a_b/status/1/?s=20&t=abc ") == "1"
    for rejected in (
        "http://x.com/founder/status/1",
        "https://twitter.com/founder/status/1",
        "https://x.com.evil.example/founder/status/1",
        "https://evil.example/https://x.com/founder/status/1",
        "https://x.com/founder/status/abc",
        "https://x.com/founder/likes",
        "https://x.com/founder/status/1#frag",
        "https://x.com/this_handle_is_too_long/status/1",
    ):
        assert parse_x_status_url(rejected) is None, rejected


async def test_manual_link_lookup_by_id() -> None:
    body = page([post("777", "Manual link text worth a look")])
    found = Recorder(
        httpx.Response(200, json={"data": body["data"][0], "includes": body["includes"]})
    )
    missing = Recorder(
        httpx.Response(200, json={"errors": [{"title": "Not Found Error", "resource_id": "9"}]})
    )

    item = await found.client().lookup_post("777")

    assert found.requests[0].url.path == "/2/tweets/777"
    assert item.source_key == "manual" and item.author_handle == "founder"
    with pytest.raises(SourceUnavailable) as error:
        await missing.client().lookup_post("9")
    assert error.value.code == "not_found"
    with pytest.raises(SourceUnavailable):
        await missing.client().lookup_post("../../2/users/me")


async def test_partial_response_keeps_valid_posts() -> None:
    broken = {"id": "5", "text": "no created_at here"}
    orphan = post("4", "author was not expanded", author_id="missing")
    body = page([post("6", "valid post with an author"), broken, orphan])
    body["errors"] = [{"title": "Authorization Error", "resource_type": "user"}]
    recorder = Recorder(httpx.Response(200, json=body))

    [fetch] = await collector(recorder).collect({})

    assert [item.external_id for item in fetch.items] == ["6", "4"]
    assert fetch.items[1].author_handle is None
    assert str(fetch.items[1].url) == "https://x.com/i/web/status/4"
    assert fetch.cursor == "6"


async def test_one_failed_query_does_not_stop_others(repository: SQLiteEventRepository) -> None:
    def route(request: httpx.Request) -> httpx.Response:
        if request.url.params["query"].startswith("bad"):
            return httpx.Response(403)
        return httpx.Response(200, json=page([post("50", "A strong public prediction was made")]))

    recorder = Recorder(route)

    counters = await pipeline(collector(recorder, "bad", "good"), repository).run_once()

    assert counters.source_errors == 1 and counters.inserted == 1
    assert await repository.get_checkpoints() == {"query:good": "50"}


async def test_repeated_run_uses_checkpoint_and_creates_no_duplicates(
    repository: SQLiteEventRepository,
) -> None:
    post_id = snowflake(minutes_ago=5)
    recorder = Recorder(
        httpx.Response(200, json=page([post(post_id, "Founder said the market will double")]))
    )
    source = collector(recorder)

    first = await pipeline(source, repository).run_once()
    second = await pipeline(source, repository).run_once()

    assert first.inserted == 1
    assert second.inserted == 0 and second.duplicates == 1
    assert recorder.requests[1].url.params["since_id"] == post_id
    assert sum((await repository.count_by_status()).values()) == 1


class FailingRepository(SQLiteEventRepository):
    async def add_events(self, events: Sequence[EventCandidate], mentions: object = ()) -> int:
        raise RuntimeError("database is locked")


async def test_database_failure_does_not_advance_checkpoint(
    repository: SQLiteEventRepository,
) -> None:
    recorder = Recorder(
        httpx.Response(200, json=page([post("91", "Founder promised a launch this week")]))
    )
    source = collector(recorder)

    with pytest.raises(RuntimeError):
        await pipeline(source, FailingRepository(repository._db_path)).run_once()
    assert await repository.get_checkpoints() == {}

    counters = await pipeline(source, repository).run_once()

    assert "since_id" not in recorder.requests[1].url.params
    assert counters.inserted == 1
    assert await repository.get_checkpoints() == {"query:main": "91"}

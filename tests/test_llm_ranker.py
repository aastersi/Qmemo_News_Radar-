import json
import logging
import os
import sqlite3
from collections import defaultdict
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest
from fakes import LLM_CALL_USD, open_guard
from pydantic import HttpUrl

from qmemo_radar.application.filtering import FilterPolicy
from qmemo_radar.application.normalization import build_candidate
from qmemo_radar.application.pipeline import PipelineThresholds, RadarPipeline
from qmemo_radar.domain import Engagement, EventStatus, RawSourceItem, SourceType
from qmemo_radar.exceptions import RankingFailed
from qmemo_radar.infrastructure.collectors import FakeCollector
from qmemo_radar.infrastructure.llm import ChatCompletionsClient
from qmemo_radar.infrastructure.ranking import (
    RANKING_SYSTEM_PROMPT,
    LlmRanker,
    ranking_user_message,
)
from qmemo_radar.infrastructure.storage import SQLiteEventRepository

FIXTURES: list[dict[str, Any]] = json.loads(
    (Path(__file__).parent / "fixtures" / "ranking_posts.json").read_text(encoding="utf-8")
)
# Reversed so the original post wins over its duplicate copy.
CATEGORY_BY_TEXT = {entry["text"]: entry["category"] for entry in reversed(FIXTURES)}
API_KEY = "sk-test-secret-key-123"

# What a well-behaved model is expected to return per fixture category.
PROFILES = {
    "strong": (27, 18, 13, 14, 9, 9, 0),
    "short_quote": (24, 17, 10, 14, 9, 8, 0),
    "no_direct_quote": (22, 8, 10, 14, 8, 6, 2),
    "weak": (12, 6, 5, 14, 6, 3, 0),
    "irrelevant": (3, 2, 3, 14, 7, 1, 0),
    "risky": (20, 14, 14, 14, 7, 6, 28),
    "injection": (5, 6, 2, 15, 4, 1, 25),
}
COMPONENTS = (
    "qmemo_relevance",
    "quote_strength",
    "discussion_potential",
    "freshness",
    "clarity",
    "action_likelihood",
    "risk_penalty",
)


def fixture_item(entry: dict[str, Any]) -> RawSourceItem:
    return RawSourceItem(
        source=SourceType.X,
        external_id=entry["key"],
        url=HttpUrl(f"https://x.com/{entry['author']}/status/{abs(hash(entry['key']))}"),
        author_handle=entry["author"],
        author_display_name=entry["name"],
        original_text=entry["text"],
        language="en",
        published_at=datetime.now(UTC) - timedelta(minutes=entry["minutes_ago"]),
        engagement=Engagement(**entry["metrics"]),
    )


def posts_in(payload: dict[str, Any]) -> list[dict[str, Any]]:
    content = payload["messages"][1]["content"]
    data = content.split("<posts>\n", 1)[1].rsplit("\n</posts>", 1)[0]
    posts: list[dict[str, Any]] = json.loads(data)
    return posts


def judged(payload: dict[str, Any]) -> str:
    scores = []
    for post in posts_in(payload):
        values = dict(zip(COMPONENTS, PROFILES[CATEGORY_BY_TEXT[post["text"]]], strict=True))
        scores.append(
            {
                "event_id": post["event_id"],
                **values,
                "total": 100,
                "headline": "Заголовок",
                "summary": "Пересказ публикации.",
                "rationale": "Объяснение связи с QMemo.",
                "recommended_format": "quote_card",
                "target_action": "save_quote",
                "fact_check_required": values["risk_penalty"] > 10,
                "fact_check_note": "Проверить факт." if values["risk_penalty"] > 10 else None,
            }
        )
    return json.dumps({"scores": scores}, ensure_ascii=False)


class FakeModel:
    def __init__(self, *replies: str) -> None:
        self.payloads: list[dict[str, Any]] = []
        self._replies: Iterator[str] = iter(replies)

    def handler(self, request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/v1/chat/completions"
        payload = json.loads(request.content)
        self.payloads.append(payload)
        content = next(self._replies, None) or judged(payload)
        return httpx.Response(200, json={"choices": [{"message": {"content": content}}]})

    def ranker(self) -> LlmRanker:
        http = httpx.AsyncClient(
            base_url="https://llm.test/v1",
            headers={"Authorization": f"Bearer {API_KEY}"},
            transport=httpx.MockTransport(self.handler),
        )
        return LlmRanker(
            ChatCompletionsClient(
                http, model="test-model", guard=open_guard(), cost_per_call_usd=LLM_CALL_USD
            )
        )


def radar(
    ranker: LlmRanker, repo: SQLiteEventRepository, items: list[RawSourceItem]
) -> RadarPipeline:
    return RadarPipeline(
        collector=FakeCollector(items),
        ranker=ranker,
        repository=repo,
        filter_policy=FilterPolicy(max_age=timedelta(minutes=60)),
        thresholds=PipelineThresholds(),
    )


def rows(repo: SQLiteEventRepository, query: str) -> list[tuple[Any, ...]]:
    with sqlite3.connect(repo._db_path) as db:
        return db.execute(query).fetchall()


async def test_fixture_priorities_filters_and_batches(repository: SQLiteEventRepository) -> None:
    model = FakeModel()
    items = [fixture_item(entry) for entry in FIXTURES]

    counters = await radar(model.ranker(), repository, items).run_once()

    assert 20 <= len(FIXTURES) <= 30
    assert [len(posts_in(payload)) for payload in model.payloads] == [10, 10, 3]
    assert (counters.filtered, counters.duplicates) == (2, 1)  # identical text is a duplicate
    assert (counters.scored, counters.rank_failed) == (23, 0)
    reasons = dict(rows(repository, "SELECT external_id, filter_reason FROM radar_events"))
    assert reasons["old-1"] == "too_old"
    assert "duplicate-1" not in reasons  # an identical text: a mention, not a row
    assert rows(repository, "SELECT COUNT(*) FROM content_mentions") == [(1,)]
    assert "noise-1" not in reasons  # rejected by the gate before storage
    assert reasons["strong-1"] is None

    totals = dict(
        rows(
            repository,
            "SELECT e.external_id, s.total FROM event_scores s "
            "JOIN radar_events e ON e.id = s.event_id",
        )
    )
    by_category: dict[str, list[int]] = defaultdict(list)
    for entry in FIXTURES:
        if entry["key"] in totals:
            by_category[entry["category"]].append(totals[entry["key"]])
    assert min(by_category["strong"]) > max(by_category["short_quote"])
    assert min(by_category["short_quote"]) > max(by_category["no_direct_quote"])
    below = by_category["weak"] + by_category["irrelevant"] + by_category["risky"]
    assert min(by_category["no_direct_quote"]) > max(below + by_category["injection"])

    shortlisted = {
        key
        for (key,) in rows(
            repository, "SELECT external_id FROM radar_events WHERE status = 'SHORTLISTED'"
        )
    }
    expected = {
        e["key"] for e in FIXTURES if e["category"] in {"strong", "short_quote", "no_direct_quote"}
    }
    assert shortlisted == expected


async def test_total_is_recomputed_by_code_not_taken_from_model() -> None:
    event = build_candidate(fixture_item(FIXTURES[0]))
    reply = {
        "scores": [
            {
                "event_id": event.event_id,
                "qmemo_relevance": 10,
                "quote_strength": 10,
                "discussion_potential": 10,
                "freshness": 10,
                "clarity": 10,
                "action_likelihood": 10,
                "risk_penalty": 5,
                "total": 100,
                "headline": "h",
                "summary": "s",
                "rationale": "r",
                "recommended_format": "quote_card",
                "target_action": "save_quote",
                "fact_check_required": False,
                "fact_check_note": None,
            }
        ]
    }

    [result] = await FakeModel(json.dumps(reply)).ranker().rank([event])

    assert result.total == 55


async def test_invalid_answer_gets_exactly_one_repair_request() -> None:
    model = FakeModel("Sure! Here are the scores you asked for.")
    events = [build_candidate(fixture_item(entry)) for entry in FIXTURES[:2]]

    results = await model.ranker().rank(events)

    assert len(results) == 2 and len(model.payloads) == 2
    repair = model.payloads[1]["messages"]
    assert repair[2] == {"role": "assistant", "content": "Sure! Here are the scores you asked for."}
    assert repair[3]["role"] == "user" and "could not be accepted" in repair[3]["content"]
    assert repair[0]["content"] == RANKING_SYSTEM_PROMPT


async def test_second_invalid_answer_means_the_event_is_never_shown(
    repository: SQLiteEventRepository,
) -> None:
    model = FakeModel("{}", '{"scores": []}')

    counters = await radar(model.ranker(), repository, [fixture_item(FIXTURES[0])]).run_once()

    assert len(model.payloads) == 2
    assert (counters.scored, counters.rank_failed, counters.shortlisted) == (0, 1, 0)
    assert await repository.count_by_status() == {EventStatus.FILTERED_OUT.value: 1}
    assert rows(repository, "SELECT filter_reason FROM radar_events") == [("rank_invalid_output",)]
    assert rows(repository, "SELECT COUNT(*) FROM event_scores") == [(0,)]


async def test_one_poisoned_post_does_not_block_its_batch(
    repository: SQLiteEventRepository,
) -> None:
    poison = "Poisoned post that always breaks the model answer, no matter what."

    def handler(request: httpx.Request) -> httpx.Response:
        payload = json.loads(request.content)
        texts = [post["text"] for post in posts_in(payload)]
        content = "garbage" if poison in texts else judged(payload)
        return httpx.Response(200, json={"choices": [{"message": {"content": content}}]})

    http = httpx.AsyncClient(base_url="https://llm.test/v1", transport=httpx.MockTransport(handler))
    CATEGORY_BY_TEXT[poison] = "weak"
    items = [fixture_item(entry) for entry in FIXTURES if entry["category"] == "strong"]
    items.append(fixture_item({**FIXTURES[11], "key": "poison", "text": poison}))

    counters = await radar(
        LlmRanker(
            ChatCompletionsClient(
                http, model="m", guard=open_guard(), cost_per_call_usd=LLM_CALL_USD
            )
        ),
        repository,
        items,
    ).run_once()

    assert counters.scored == 5 and counters.rank_failed == 1
    statuses = dict(rows(repository, "SELECT external_id, status FROM radar_events"))
    assert statuses.pop("poison") == "FILTERED_OUT"
    assert set(statuses.values()) == {"SHORTLISTED"}


async def test_unreachable_provider_keeps_events_for_the_next_run(
    repository: SQLiteEventRepository,
) -> None:
    calls: list[int] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(1)
        return httpx.Response(503)

    async def no_sleep(seconds: float) -> None:
        return None

    http = httpx.AsyncClient(base_url="https://llm.test/v1", transport=httpx.MockTransport(handler))
    ranker = LlmRanker(ChatCompletionsClient(
        http, model="m", guard=open_guard(), cost_per_call_usd=LLM_CALL_USD, sleep=no_sleep
    ))
    items = [fixture_item(entry) for entry in FIXTURES[:12]]

    counters = await radar(ranker, repository, items).run_once()

    assert len(calls) == 3  # one batch, three attempts, then ranking stops for this run
    assert counters.rank_failed == 10
    assert await repository.count_by_status() == {EventStatus.DISCOVERED.value: 12}


async def test_foreign_or_missing_event_ids_are_rejected() -> None:
    events = [build_candidate(fixture_item(entry)) for entry in FIXTURES[:2]]
    valid = json.loads(judged({"messages": [{}, {"content": ranking_user_message(events)}]}))
    foreign = {"scores": [*valid["scores"], {**valid["scores"][0], "event_id": "hack-1"}]}
    missing = {"scores": valid["scores"][:1]}
    model = FakeModel(json.dumps(foreign), json.dumps(missing))

    with pytest.raises(RankingFailed) as error:
        await model.ranker().rank(events)

    assert error.value.code == "invalid_llm_output"
    assert len(model.payloads) == 2


async def test_prompt_injection_stays_inside_the_untrusted_data_block() -> None:
    model = FakeModel()
    injections = [entry for entry in FIXTURES if entry["category"] == "injection"]
    events = [build_candidate(fixture_item(entry)) for entry in injections]

    results = await model.ranker().rank(events)

    system, user = model.payloads[0]["messages"]
    assert system == {"role": "system", "content": RANKING_SYSTEM_PROMPT}
    assert user["content"].count("<posts>") == 1 and user["content"].count("</posts>") == 1
    assert [post["text"] for post in posts_in(model.payloads[0])] == [
        e.original_text for e in events
    ]
    for event in events:
        assert event.original_text not in system["content"]
    assert all(result.total < 65 for result in results)
    assert {result.event_id for result in results} == {event.event_id for event in events}


async def test_request_uses_zero_temperature_and_score_keeps_model_and_prompt_version(
    repository: SQLiteEventRepository,
) -> None:
    model = FakeModel()

    await radar(model.ranker(), repository, [fixture_item(FIXTURES[0])]).run_once()

    assert model.payloads[0]["temperature"] == 0.0
    assert model.payloads[0]["model"] == "test-model"
    assert rows(repository, "SELECT prompt_version, model_name FROM event_scores") == [
        ("rank-v1", "test-model")
    ]


async def test_logs_contain_neither_key_nor_prompt_nor_post_text(
    repository: SQLiteEventRepository, caplog: pytest.LogCaptureFixture
) -> None:
    caplog.set_level(logging.DEBUG, logger="qmemo_radar")
    caplog.set_level(logging.INFO, logger="httpx")
    model = FakeModel("not json")
    items = [fixture_item(entry) for entry in FIXTURES[:3]]

    await radar(model.ranker(), repository, items).run_once()

    assert any(record.levelno == logging.WARNING for record in caplog.records)
    logged = "\n".join(f"{record.getMessage()} {record.__dict__}" for record in caplog.records)
    assert API_KEY not in logged
    assert RANKING_SYSTEM_PROMPT[:60] not in logged
    for item in items:
        assert item.original_text not in logged


@pytest.mark.live
@pytest.mark.skipif(
    os.environ.get("RADAR_LIVE_LLM_EVAL") != "1",
    reason="set RADAR_LIVE_LLM_EVAL=1 and RADAR_LLM_* to evaluate the real model",
)
async def test_live_model_respects_fixture_priorities() -> None:
    from qmemo_radar.bootstrap import build_llm_client, build_llm_http_client
    from qmemo_radar.config import RadarSettings

    settings = RadarSettings()
    ranked_categories = set(PROFILES)
    entries = [entry for entry in FIXTURES if entry["category"] in ranked_categories]
    candidates = [build_candidate(fixture_item(entry)) for entry in entries]
    async with build_llm_http_client(settings) as http:
        ranker = LlmRanker(build_llm_client(settings, http, open_guard()))
        results = [
            result
            for start in range(0, len(candidates), 10)
            for result in await ranker.rank(candidates[start : start + 10])
        ]
    category = {c.event_id: e["category"] for c, e in zip(candidates, entries, strict=True)}
    average: dict[str, float] = {}
    for name in ranked_categories:
        totals = [r.total for r in results if category[r.event_id] == name]
        average[name] = sum(totals) / len(totals)
    assert average["strong"] > average["weak"]
    assert average["strong"] > average["irrelevant"]
    assert average["strong"] > average["risky"]
    assert average["strong"] > average["injection"]
    assert all(r.total < 65 for r in results if category[r.event_id] == "injection")

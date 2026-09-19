"""Selection rules: gate, near duplicates, same-event matching, false merges, preselection."""

import re
from datetime import UTC, datetime, timedelta

import pytest

from qmemo_radar.application.normalization import build_candidate
from qmemo_radar.application.selection import (
    ClusterSignals,
    SelectionPolicy,
    article_title,
    band_keys,
    compare,
    features,
    free_score,
    gate_reason,
    preselect,
    topic,
)
from qmemo_radar.config import DEFAULT_TEMPLATE_PATTERNS, SourcesConfig
from qmemo_radar.domain import RawSourceItem, SourceType

NOW = datetime(2026, 9, 16, 12, 0, tzinfo=UTC)
POLICY = SelectionPolicy(
    templates=tuple(re.compile(pattern) for pattern in DEFAULT_TEMPLATE_PATTERNS),
    blocked_domains=("spam.example",),
    blocked_terms=("giveaway",),
    languages=frozenset({"english", "en"}),
)


def item(
    text: str, url: str = "https://news.example/a", language: str | None = "ENGLISH"
) -> RawSourceItem:
    return RawSourceItem(
        source=SourceType.GDELT,
        external_id=str(abs(hash((text, url)))),
        url=url,
        original_text=text,
        language=language,
        published_at=NOW,
    )


@pytest.mark.parametrize(
    ("text", "url", "language", "reason"),
    [
        ("Gulf of America", None, "ENGLISH", "too_short"),
        ("Absolutely unprecedented catastrophic failure", None, "ENGLISH", "too_few_words"),
        ("x" * 5001, None, "ENGLISH", "too_long"),
        ("Una frase bastante larga para el radar hoy", None, "SPANISH", "language"),
        (
            "A normal sentence from a blocked website today",
            "https://news.spam.example/a",
            "en",
            "blocked_domain",
        ),
        (
            "An old story republished by the aggregator today",
            "https://old.example/2023/05/02/story",
            "en",
            "stale_url",
        ),
        ("Fri, 23 Aug 2126 07:07:22 -0400 and more words", None, "en", "template"),
        ("Click here to read the whole story about it", None, "en", "template"),
        ("Join the giveaway and win a car this weekend", None, "en", "blocked_term"),
        ("12:30 14:45 16:00 18:15 20:30 22:45 1-2 3-4", None, "en", "low_information"),
        ("no no no no no no no no no no no no", None, "en", "low_information"),
    ],
)
def test_the_gate_rejects_obvious_noise_with_a_reason(
    text: str, url: str | None, language: str, reason: str
) -> None:
    assert (
        gate_reason(item(text, url or "https://news.example/a", language), POLICY, now=NOW)
        == reason
    )


def test_the_gate_keeps_real_statements_and_unknown_languages() -> None:
    keep = [
        item("We will not raise taxes this year, whatever happens in parliament"),
        item("A statement with no language metadata at all", language=None),
        item("A fresh story in a dated URL path today", "https://ok.example/2026/09/16/story"),
    ]
    assert [gate_reason(entry, POLICY, now=NOW) for entry in keep] == [None, None, None]


def test_topics_can_be_required_without_code_changes() -> None:
    policy = SelectionPolicy(
        require_topic=True, topics=(topic("ai", ["artificial intelligence", "AI"], 5),)
    )
    assert (
        gate_reason(item("Artificial intelligence will change every job"), policy, now=NOW) is None
    )
    assert (
        gate_reason(item("The bridge will reopen after the long repairs"), policy, now=NOW)
        == "off_topic"
    )
    # Whole words only: "maid" does not contain the topic "ai".
    assert (
        gate_reason(item("The maid said the house is finally clean"), policy, now=NOW)
        == "off_topic"
    )


def test_selection_lives_in_sources_yaml() -> None:
    config = SourcesConfig.model_validate(
        {
            "selection": {
                "gate": {"min_words": 7, "blocked_domains": ["spam.example"]},
                "topics": [{"name": "crypto", "terms": ["bitcoin", "ether"], "weight": 8}],
                "clustering": {"same_event_min_shared": 5},
                "preselection": {"min_score": 55},
            }
        }
    )
    assert config.selection.gate.min_words == 7
    assert config.selection.topics[0].weight == 8
    with pytest.raises(ValueError, match="require_topic"):
        SourcesConfig.model_validate({"selection": {"gate": {"require_topic": True}}})
    with pytest.raises(ValueError):
        SourcesConfig.model_validate({"selection": {"gate": {"template_patterns": ["(unclosed"]}}})


SAME_STORY = [
    # A paraphrase with a different unit spelling: one claim, one cluster.
    ("CEO says Bitcoin will reach $200k this year", "CEO: Bitcoin could hit $200,000 this year"),
    # GDELT quote spans of different length from two outlets.
    (
        "We encourage Iran and the US to stay rational and restrained,",
        "We encourage Iran and the US to stay rational and restrained and to resolve differences",
    ),
    (
        "Elon Musk announced the new rocket will launch in March",
        "Musk says the new rocket will launch in March",
    ),
    (
        "Our hearts are with the victims and their families tonight.",
        "our hearts are with the victims and their families",
    ),
]

DIFFERENT_STORIES = [
    ("CEO says Bitcoin will reach $200k this year", "CEO says Bitcoin will reach $100k this year"),
    ("CEO says Bitcoin will reach $200k this year", "CEO says Ethereum will reach $200k this year"),
    (
        "We will raise taxes for the richest households next year",
        "We will not raise taxes for the richest households next year",
    ),
    ("Bitcoin will reach $200k this year", "Bitcoin will fall below $200k this year"),
    ("The minister said the budget is final", "The minister said the budget is a disgrace"),
    # One different lowercase noun each: nothing tells a synonym from another subject, so apart.
    (
        'The minister said: "Budget item 1 of gdelt is final."',
        'The minister said: "Budget item 1 of rss is final."',
    ),
    (
        "Apple will open a factory in Texas next year",
        "Apple will close its factory in Texas next year",
    ),
    (
        "Shares rose sharply after the earnings report today",
        "Shares fell sharply after the earnings report today",
    ),
    (
        "The council approved the new stadium plan on Monday",
        "The council rejected the new stadium plan on Monday",
    ),
]


@pytest.mark.parametrize(("a", "b"), SAME_STORY)
def test_variants_of_one_statement_are_one_story(a: str, b: str) -> None:
    kind, _ = compare(features(a), features(b), SelectionPolicy())
    assert kind in ("near_duplicate", "same_event")


@pytest.mark.parametrize(("a", "b"), DIFFERENT_STORIES)
def test_similar_but_different_statements_stay_apart(a: str, b: str) -> None:
    assert compare(features(a), features(b), SelectionPolicy()) == (None, 0.0)


@pytest.mark.parametrize(
    ("a", "b"),
    [
        (
            "The Echidna | It's the war, stupid | Camden Haven Courier",
            "The Echidna | It's the war, stupid | Goulburn Post",
        ),
        (
            "The sucker's dilemma: The psychology of getting conned – HOT 105!",
            "The sucker's dilemma: The psychology of getting conned – KISS 98.5",
        ),
        (
            "Noah was 'my greatest gift in life' – Fiona Donohoe | Epping Forest Guardian",
            "Noah was 'my greatest gift in life' – Fiona Donohoe | Hexham Courant",
        ),
    ],
)
def test_a_network_reprint_with_its_site_name_is_the_same_article(a: str, b: str) -> None:
    assert article_title(a) == article_title(b)


def test_a_title_is_not_cut_to_a_stub() -> None:
    assert article_title("Oil prices - live") == "oil prices - live"
    assert article_title("Fed raises rates | Reuters") == "fed raises rates"
    assert article_title("US-China talks resume in Geneva") == "us-china talks resume in geneva"


def test_near_duplicates_are_found_by_the_band_index() -> None:
    a = features("Artists must not be silenced when they speak up for the oppressed,")
    b = features("Artists must not be silenced when they speak up for the oppressed.")
    c = features("Artists must not be silenced when they speak up for the oppressed people")
    assert compare(a, c, SelectionPolicy())[0] == "near_duplicate"
    assert band_keys(a.tokens) == band_keys(b.tokens)  # identical words: every band
    assert set(band_keys(a.tokens)) & set(band_keys(c.tokens))
    assert band_keys(features("too short").tokens) == ()


def signals(text: str, **values: object) -> ClusterSignals:
    event = build_candidate(item(text), discovered_at=NOW - timedelta(minutes=20))
    return ClusterSignals(event=event, **values)  # type: ignore[arg-type]


def test_preselection_rewards_independent_sources_and_claims_and_explains_itself() -> None:
    claim = "We will cut emissions by 50% before 2030, I promise you that"
    lone = preselect(signals(claim), SelectionPolicy(), now=NOW)
    wide = preselect(
        signals(claim, mentions=40, domains=25, articles=25, articles_last_hour=12),
        SelectionPolicy(),
        now=NOW,
    )
    plain = preselect(
        signals("the weather was nice and people enjoyed the fair a lot"),
        SelectionPolicy(),
        now=NOW,
    )

    assert wide.total > lone.total > plain.total
    assert wide.breakdown.discussion_potential == 15 and lone.breakdown.discussion_potential == 0
    explained = " ".join(wide.notes)
    assert "прогноз" in explained and "обещание" in explained and "25 статей" in explained
    assert wide.breakdown.action_likelihood == 10 and plain.breakdown.action_likelihood == 0


def test_a_quote_from_one_site_alone_never_reaches_the_digest() -> None:
    # Independent outlets are the evidence that a quote matters; wording alone is not enough.
    strongest = signals(
        'Elon Musk: "We will land 1,000 Starships on Mars by 2030, I promise, never doubt it."',
        articles_last_hour=1,
        members=1,
    )
    assert preselect(strongest, SelectionPolicy(), now=NOW).total < 65
    wider = signals(
        strongest.event.original_text, domains=4, articles=4, mentions=6, articles_last_hour=4
    )
    assert preselect(wider, SelectionPolicy(), now=NOW).total >= 65


def test_one_site_repeating_a_text_is_boilerplate() -> None:
    result = preselect(
        signals(
            "Our team does a great job making sure every threat is found", mentions=300, domains=1
        ),
        SelectionPolicy(),
        now=NOW,
    )
    assert result.boilerplate and result.breakdown.risk_penalty == 30


def test_the_free_score_is_the_preselection_in_the_ranking_shape() -> None:
    data = signals(
        "We will cut emissions by 50% before 2030, I promise you that",
        mentions=40,
        domains=25,
        articles=20,
        articles_last_hour=12,
        sample_domains=("bbc.co.uk", "reuters.com"),
    )
    score = free_score(data, SelectionPolicy(), now=NOW)
    assert score.total == preselect(data, SelectionPolicy(), now=NOW).total
    assert score.fact_check_required and score.model_name == "none"
    assert "20 статьях на 25 сайтах" in score.summary and "bbc.co.uk" in score.summary
    assert score.rationale.startswith("Бесплатная оценка без LLM")

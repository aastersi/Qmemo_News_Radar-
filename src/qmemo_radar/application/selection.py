"""Selection: what is worth keeping, which texts tell one story, and which stories to rank.

Everything here is deterministic, local and free. The chain per collection run:
gate (before storage) -> exact copies become mentions -> near duplicates and same-event texts
join one cluster -> the cluster gets an explainable preselection score -> only preselected
clusters are ranked. The score is on the ranking scale, so without a paid LLM it is the rank.
"""

import hashlib
import math
import re
import struct
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from functools import lru_cache
from typing import Literal
from urllib.parse import urlsplit

from qmemo_radar.application.normalization import comparison_text
from qmemo_radar.domain import EventCandidate, RawSourceItem, ScoreBreakdown, ScoreResult

# ---------------------------------------------------------------------------------------------
# Policy (built from sources.yaml `selection`, see config.SelectionConfig)


@dataclass(frozen=True, slots=True)
class Topic:
    name: str
    pattern: re.Pattern[str]
    weight: int


@dataclass(frozen=True, slots=True)
class SelectionPolicy:
    min_words: int = 5
    min_chars: int = 25
    max_chars: int = 5_000
    languages: frozenset[str] = frozenset()
    blocked_terms: tuple[str, ...] = ()
    blocked_domains: tuple[str, ...] = ()
    templates: tuple[re.Pattern[str], ...] = ()
    min_letter_ratio: float = 0.6
    max_url_age: timedelta | None = timedelta(days=30)
    require_topic: bool = False
    topics: tuple[Topic, ...] = ()
    window: timedelta = timedelta(hours=48)
    near_duplicate_jaccard: float = 0.8
    same_event_overlap: float = 1.0
    same_event_min_shared: int = 4
    min_preselect_score: int = 55
    always_rank_sources: frozenset[str] = frozenset({"x"})
    boilerplate_min_mentions: int = 10
    boilerplate_max_domains: int = 2


def topic(name: str, terms: Iterable[str], weight: int) -> Topic:
    words = "|".join(re.escape(comparison_text(term)) for term in terms)
    return Topic(name, re.compile(rf"(?<!\w)(?:{words})(?!\w)"), weight)


# ---------------------------------------------------------------------------------------------
# Gate: runs on the raw item, before a row, a hash or a normalized copy exists.

_URL_DATE = re.compile(
    r"/((?:19|20)\d{2})[/-](0[1-9]|1[0-2])(?:[/-](0[1-9]|[12]\d|3[01]))?(?=[/-])"
)


def gate_reason(item: RawSourceItem, policy: SelectionPolicy, *, now: datetime) -> str | None:
    """Why this item is not worth storing, or None. Cheapest checks first."""
    text = item.original_text
    if len(text) > policy.max_chars:
        return "too_long"
    if len(text.strip()) < policy.min_chars:
        return "too_short"
    words = text.split()
    if len(words) < policy.min_words:
        return "too_few_words"
    if policy.languages and item.language and not _language_allowed(item.language, policy):
        return "language"
    domain = domain_of(str(item.url))
    if any(
        domain == blocked or domain.endswith("." + blocked) for blocked in policy.blocked_domains
    ):
        return "blocked_domain"
    if policy.max_url_age and _url_date_before(str(item.url), now - policy.max_url_age):
        return "stale_url"
    lowered = comparison_text(text)
    if any(pattern.search(lowered) for pattern in policy.templates):
        return "template"
    if any(term in lowered for term in policy.blocked_terms):
        return "blocked_term"
    if _low_information(text, words, policy.min_letter_ratio):
        return "low_information"
    if policy.require_topic and not topic_hits(lowered, policy):
        return "off_topic"
    return None


def _language_allowed(language: str, policy: SelectionPolicy) -> bool:
    value = language.strip().casefold()
    return value in policy.languages or value.split("-")[0] in policy.languages


def _url_date_before(url: str, limit: datetime) -> bool:
    match = _URL_DATE.search(urlsplit(url).path)
    if not match:
        return False
    year, month, day = int(match[1]), int(match[2]), int(match[3] or 28)
    return (year, month, day) < (limit.year, limit.month, limit.day)


def _low_information(text: str, words: Sequence[str], min_letter_ratio: float) -> bool:
    visible = [char for char in text if not char.isspace()]
    letters = sum(char.isalpha() for char in visible)
    if letters < min_letter_ratio * len(visible):
        return True
    unique = {word.casefold().strip(".,;:!?\"'") for word in words}
    # "no no no no no no" or a list of the same tag repeated.
    return len(words) >= 8 and len(unique) < len(words) / 3


def article_of(item: RawSourceItem) -> str:
    """Which article a text comes from: its title when the source gives one (GDELT, RSS), else
    its URL. Syndicated reprints keep the title, so they count as one article."""
    title = item.raw_payload.get("title")
    if isinstance(title, str) and title.strip():
        key = article_title(title)
    else:
        key = str(item.url)
    return hashlib.blake2b(key.encode(), digest_size=8).hexdigest()


_SITE_SUFFIX = re.compile(r"\s+[|–—-]\s+(?!.*\s[|–—-]\s)")


def article_title(title: str) -> str:
    """The title without a site name appended by a newspaper network ("Story | Camden Courier",
    "Story – HOT 105!"): measured on the real replay, such reprints otherwise counted as dozens
    of articles. Kept when the rest would be under three words."""
    normalized = comparison_text(title)
    head = _SITE_SUFFIX.split(normalized, maxsplit=1)[0] if _SITE_SUFFIX.search(normalized) else ""
    return head if len(head.split()) >= 3 else normalized


def domain_of(url: str) -> str:
    host = (urlsplit(url).hostname or "").lower()
    return host.removeprefix("www.")


def topic_hits(normalized: str, policy: SelectionPolicy) -> tuple[Topic, ...]:
    return tuple(rule for rule in policy.topics if rule.pattern.search(normalized))


# ---------------------------------------------------------------------------------------------
# Text comparison

_WORD = re.compile(r"[^\W_]+(?:['’][^\W_]+)?")
_NUMBER = re.compile(
    r"(?<![\w.])(\d{1,3}(?:,\d{3})+|\d+(?:\.\d+)?)\s*(k|m|bn|million|billion|thousand|%)?(?!\w)"
)
_SCALE = {"k": 1e3, "thousand": 1e3, "m": 1e6, "million": 1e6, "bn": 1e9, "billion": 1e9}
_NEGATIONS = frozenset(
    "not no never nobody nothing none neither nor cannot can't won't don't doesn't didn't isn't "
    "aren't wasn't weren't shouldn't wouldn't couldn't haven't hasn't hadn't ain't".split()
)
_POLARITY = frozenset(
    "up down above below before after more less over under against without off out".split()
)
# English function words and reporting verbs: they carry no story, only phrasing.
STOPWORDS = frozenset(
    """a about above after again against all am an and any are as at be because been before being
below between both but by can could did do does doing down during each few for from further had
has have having he her here hers herself him himself his how i if in into is it its itself just
me more most my myself of off on once only or other our ours ourselves out over own same she
should so some such than that the their theirs them themselves then there these they this those
through to too under until up very was we were what when where which while who whom why will
with would you your yours yourself yourselves s t now also get got go going one like well really
think know us it's i'm we're that's there's we've i've they're you're i'll we'll they'll he's
she's let's may might must shall said say says saying told tells tell according added adds
stated states announced announces""".split()
)


# Words that newsrooms swap when retelling one statement ("will reach $200k" / "could hit
# $200,000"): mapped to one form before comparison. Only close synonyms; never opposites.
_SYNONYM_GROUPS = (
    ("reach", "hit", "hits", "reaches", "reached", "top", "tops", "surpass", "surpasses"),
    ("rise", "rises", "climb", "climbs", "jump", "jumps", "soar", "soars", "surge", "surges"),
    ("fall", "falls", "drop", "drops", "slide", "slides", "plunge", "plunges", "sink", "sinks"),
    ("buy", "buys", "acquire", "acquires", "purchase", "purchases"),
    ("quit", "quits", "resign", "resigns", "resigned", "step", "steps"),
    ("launch", "launches", "launched", "unveil", "unveils", "unveiled", "introduce"),
    ("ban", "bans", "banned", "prohibit", "prohibits", "outlaw", "outlaws"),
    ("kill", "kills", "killed", "dead", "died", "dies"),
    ("year", "years", "yearly", "annual"),
)
_SYNONYM = {word: group[0] for group in _SYNONYM_GROUPS for word in group}


@dataclass(frozen=True, slots=True)
class TextFeatures:
    """What two texts are compared on. Computed from the original text of an event."""

    tokens: frozenset[str]
    numbers: frozenset[str]
    negated: bool
    entities: frozenset[str]
    # Direction words (up/down, before/after, against...): function words for similarity, but
    # the same sentence with another one says something else.
    polarity: frozenset[str] = frozenset()
    # Proper nouns in order: "Russia attacked Ukraine" is not "Ukraine attacked Russia".
    entity_order: tuple[str, ...] = ()


def features(text: str) -> TextFeatures:
    numbers = frozenset(_number_value(match) for match in _NUMBER.finditer(text))
    lowered = _NUMBER.sub(" ", comparison_text(text).replace("’", "'"))
    words = _WORD.findall(lowered)
    tokens = frozenset(
        _SYNONYM.get(word, word)
        for word in words
        if word not in STOPWORDS and word not in _NEGATIONS
    )
    order = _entity_order(text)
    return TextFeatures(
        tokens=tokens | {f"#{number}" for number in numbers},
        numbers=numbers,
        negated=any(word in _NEGATIONS or word.endswith("n't") for word in words),
        entities=frozenset(order),
        polarity=frozenset(word for word in words if word in _POLARITY),
        entity_order=tuple(dict.fromkeys(order)),
    )


def _number_value(match: re.Match[str]) -> str:
    value = float(match[1].replace(",", ""))
    unit = (match[2] or "").lower()
    if unit == "%":
        return f"{value:g}%"
    return f"{value * _SCALE.get(unit, 1):g}"


_CAPITALIZED = re.compile(r"(?<![.!?]\s)(?<!^)\b[A-Z][\w'’-]+")


def _entity_order(text: str) -> list[str]:
    """Capitalized words not starting a sentence: a crude but predictable proper-noun guess."""
    stripped = text.strip().lstrip("\"'“‘(")
    return list(
        word.casefold().removesuffix("'s").removesuffix("’s")
        for word in _CAPITALIZED.findall(stripped)
        if word not in ("I", "I'm", "I've", "I'll", "I'd")
    )


Match = Literal["near_duplicate", "same_event"]


def compare(
    a: TextFeatures, b: TextFeatures, policy: SelectionPolicy
) -> tuple[Match | None, float]:
    """Whether b tells the same story as a. Guards first: different numbers, a negation on one
    side only or a proper noun the other text lacks mean different statements."""
    if len(a.tokens) < 3 or len(b.tokens) < 3:
        return None, 0.0
    if a.numbers != b.numbers or a.negated != b.negated or a.polarity != b.polarity:
        return None, 0.0
    small, large = sorted((a.entities, b.entities), key=len)
    if not small <= large:
        return None, 0.0
    if a.entities == b.entities and len(a.entities) > 1 and a.entity_order != b.entity_order:
        return None, 0.0
    if _opposed(a.tokens - b.tokens, b.tokens - a.tokens):
        return None, 0.0
    shared = len(a.tokens & b.tokens)
    overlap = shared / min(len(a.tokens), len(b.tokens))
    # A word swapped on both sides (approve/block, five/ten, rising/slowing) can flip the
    # statement, and nothing here tells a synonym from an opposite: by default (1.0) one text
    # must lie wholly inside the other, after the synonym map.
    if overlap < policy.same_event_overlap:
        return None, 0.0
    jaccard = shared / len(a.tokens | b.tokens)
    if jaccard >= policy.near_duplicate_jaccard:
        return "near_duplicate", jaccard
    # A short text inside a longer one (another outlet quoted less) needs fewer shared words.
    if shared >= policy.same_event_min_shared or (overlap == 1.0 and shared >= 3):
        return "same_event", overlap
    return None, 0.0


# Opposite verbs and adjectives: a text differing from another only by one of these says the
# opposite. Words are compared by a crude stem (open, opens, opened, opening -> open).
_OPPOSITES = (
    ("open", "close"),
    ("rise", "fall"),
    ("raise", "cut"),
    ("raise", "lower"),
    ("increase", "decrease"),
    ("increase", "cut"),
    ("win", "lose"),
    ("gain", "loss"),
    ("buy", "sell"),
    ("hire", "fire"),
    ("start", "stop"),
    ("begin", "end"),
    ("add", "remove"),
    ("allow", "ban"),
    ("approve", "reject"),
    ("accept", "reject"),
    ("support", "oppose"),
    ("agree", "disagree"),
    ("up", "down"),
    ("above", "below"),
    ("high", "low"),
    ("higher", "lower"),
    ("more", "less"),
    ("guilty", "innocent"),
    ("true", "false"),
    ("legal", "illegal"),
    ("resign", "stay"),
    ("confirm", "deny"),
    ("expand", "shrink"),
    ("strong", "weak"),
    ("peace", "war"),
    ("rise", "drop"),
    ("rise", "decline"),
    ("grow", "shrink"),
)
_OPPOSITE_OF: dict[str, set[str]] = {}
for _left, _right in _OPPOSITES:
    _OPPOSITE_OF.setdefault(_left, set()).add(_right)
    _OPPOSITE_OF.setdefault(_right, set()).add(_left)


_IRREGULAR = {
    "rose": "rise",
    "risen": "rise",
    "fell": "fall",
    "fallen": "fall",
    "won": "win",
    "lost": "lose",
    "sold": "sell",
    "bought": "buy",
    "began": "begin",
    "begun": "begin",
    "grew": "grow",
    "grown": "grow",
    "shrank": "shrink",
    "shrunk": "shrink",
    "fewer": "less",
}


def _stem(word: str) -> str:
    if word in _IRREGULAR:
        return _IRREGULAR[word]
    for suffix in ("ing", "ed", "es", "s"):
        if word.endswith(suffix) and len(word) - len(suffix) >= 3:
            word = word[: -len(suffix)]
            break
    # closed -> clos -> close, raised -> rais -> raise, stopped -> stopp -> stop
    if word.endswith(("pp", "nn", "tt", "gg")):
        word = word[:-1]
    return word if word in _OPPOSITE_OF else word + "e" if word + "e" in _OPPOSITE_OF else word


def _opposed(only_a: frozenset[str], only_b: frozenset[str]) -> bool:
    stems_b = {_stem(word) for word in only_b}
    return any(_OPPOSITE_OF.get(_stem(word), set()) & stems_b for word in only_a)


# MinHash-LSH: 8 bands of 4 hashes over the content words. Measured on the real 24 h GDELT
# corpus (183k texts): 99.9 % of Jaccard >= 0.8 pairs share a band, 137k candidate pairs;
# SimHash found 73 %, 16x2 bands produced 8.9M candidates (docs/MULTI_SOURCE.md, section 8).
BANDS = 8
ROWS = 4


@lru_cache(maxsize=200_000)
def _token_hashes(token: str) -> tuple[int, ...]:
    data = hashlib.blake2b(token.encode(), digest_size=64).digest()
    data += hashlib.blake2b(token.encode(), digest_size=64, person=b"minhash").digest()
    return struct.unpack("<32I", data)[: BANDS * ROWS]


def band_keys(tokens: frozenset[str]) -> tuple[int, ...]:
    """One signed 64-bit key per band; texts sharing a key are compared exactly."""
    if len(tokens) < 3:
        return ()
    signature = [min(column) for column in zip(*(_token_hashes(t) for t in tokens), strict=True)]
    keys = []
    for band in range(BANDS):
        values = struct.pack("<B4I", band, *signature[band * ROWS : (band + 1) * ROWS])
        keys.append(
            int.from_bytes(hashlib.blake2b(values, digest_size=8).digest(), "little", signed=True)
        )
    return tuple(keys)


# ---------------------------------------------------------------------------------------------
# Preselection: an explainable score on the ranking scale.

_PREDICTION = re.compile(
    r"\b(will|won't|going to|gonna|expect(?:s|ed)?|predict(?:s|ed)?|forecast|by (?:19|20)\d{2}|"
    r"next (?:year|month|week|decade)|within (?:a|the next|\d+) (?:year|month|week|decade)s?|"
    r"likely to|set to|on track to)\b"
)
_PROMISE = re.compile(
    r"\b(promise[sd]?|pledge[sd]?|vow(?:s|ed)?|commit(?:s|ted)? to|guarantee[sd]?|we will|"
    r"i will|we'll|i'll|we are going to|i am going to|we're going to|i'm going to)\b"
)
_STRONG = re.compile(
    r"\b(never|always|biggest|worst|best|largest|first time|record|historic|unprecedented|"
    r"must|cannot|can't|no one|nobody|everyone|every single|the end of|end of the)\b"
)


@dataclass(frozen=True, slots=True)
class ClusterSignals:
    """What preselection and the free ranker know about one story."""

    event: EventCandidate
    cluster_id: int | None = None
    mentions: int = 1
    domains: int = 1
    articles: int = 1
    # Distinct articles seen in the last hour.
    sources: int = 1
    members: int = 1
    first_seen: datetime | None = None
    articles_last_hour: int = 1
    sample_domains: tuple[str, ...] = ()
    state: str = "candidate"


@dataclass(frozen=True, slots=True)
class Preselection:
    breakdown: ScoreBreakdown
    total: int
    notes: tuple[str, ...] = field(default=())
    topics: tuple[str, ...] = field(default=())
    boilerplate: bool = False


def preselect(signals: ClusterSignals, policy: SelectionPolicy, *, now: datetime) -> Preselection:
    """Score one story from what is known for free. Calibrated on the real 24 h GDELT replay
    (docs/MULTI_SOURCE.md, section 8): independent outlets are the main evidence that a quote
    matters, so a quote from one site alone stays below the digest threshold (65) however
    strong its wording; wording decides among quotes that several outlets carry."""
    event = signals.event
    text = event.original_text
    normalized = event.normalized_text
    words = len(text.split())
    feats = features(text)
    notes: list[str] = []

    # Notability: independent articles carrying the story (1 -> 0, 2 -> 5, 8 -> 15, 64 -> 30).
    # Articles, not sites: one wire story reprinted by 500 local sites is one article.
    hits = topic_hits(normalized, policy)
    topic_points = min(10, sum(rule.weight for rule in hits))
    reach = min(30, round(5 * math.log2(max(1, signals.articles))))
    relevance = min(30, reach + topic_points)
    notes.append(
        f"значимость {relevance}/30 ({signals.articles} статей на {signals.domains} сайтах"
        + "".join(f", тема {rule.name}" for rule in hits)
        + ")"
    )

    # Wording: a prediction, promise or strong claim, with numbers and names to hold it to.
    markers = [
        name
        for name, pattern in (
            ("прогноз", _PREDICTION),
            ("обещание", _PROMISE),
            ("сильное утверждение", _STRONG),
        )
        if pattern.search(normalized)
    ]
    claim = min(
        10,
        (8 if {"прогноз", "обещание"} & set(markers) else 0)
        + (4 if "сильное утверждение" in markers else 0),
    )
    specificity = min(4, 2 * len(feats.numbers)) + min(4, len(feats.entities))
    strength = min(20, claim + specificity + (2 if 8 <= words <= 45 else 0))
    notes.append(
        f"сила {strength}/20 ({', '.join(markers) or 'без маркеров'}; "
        f"чисел {len(feats.numbers)}, имён {len(feats.entities)}, {words} слов)"
    )

    # Momentum: articles in the last hour and distinct wordings of the story.
    momentum = round(5 * math.log2(max(1, signals.articles_last_hour)))
    discussion = min(15, momentum + 2 * (signals.members - 1))
    notes.append(
        f"обсуждение {discussion}/15 ({signals.articles_last_hour} статей за час, "
        f"{signals.mentions} всего, {signals.members} вариантов текста)"
    )

    age = now - (signals.first_seen or event.discovered_at)
    freshness = next((points for limit, points in _FRESHNESS if age <= limit), 0)
    notes.append(f"свежесть {freshness}/15 ({_hours(age)})")

    density = len(feats.tokens) / max(1, words)
    clarity = min(
        10,
        (5 if density >= 0.4 else 2)
        + (3 if text[:1].isupper() else 0)
        + (2 if text.rstrip()[-1:] in ".!\"”'" else 0),
    )
    action = 10 if claim >= 8 else 5 if markers else 0

    boilerplate = (
        signals.mentions >= policy.boilerplate_min_mentions
        and signals.domains <= policy.boilerplate_max_domains
    )
    risk = (30 if boilerplate else 0) + (5 if text.rstrip().endswith("?") else 0)
    if boilerplate:
        notes.append(f"шаблон сайта: {signals.mentions} упоминаний на {signals.domains} сайтах")

    breakdown = ScoreBreakdown(
        qmemo_relevance=relevance,
        quote_strength=strength,
        discussion_potential=discussion,
        freshness=freshness,
        clarity=clarity,
        action_likelihood=action,
        risk_penalty=min(30, risk),
    )
    total = (
        relevance + strength + discussion + freshness + clarity + action - breakdown.risk_penalty
    )
    return Preselection(
        breakdown,
        max(0, min(100, total)),
        tuple(notes),
        tuple(rule.name for rule in hits),
        boilerplate,
    )


_FRESHNESS = (
    (timedelta(hours=1), 15),
    (timedelta(hours=3), 12),
    (timedelta(hours=6), 9),
    (timedelta(hours=12), 6),
    (timedelta(hours=24), 3),
)


def _hours(age: timedelta) -> str:
    return f"{max(0, int(age.total_seconds() // 3600))} ч"


# ---------------------------------------------------------------------------------------------
# Free ranking: the preselection score, explained, in the shape the review flow expects.

FREE_RANKER_VERSION = "free-rank-v1"


def free_score(signals: ClusterSignals, policy: SelectionPolicy, *, now: datetime) -> ScoreResult:
    result = preselect(signals, policy, now=now)
    event = signals.event
    where = ", ".join(signals.sample_domains[:3]) or domain_of(str(event.url))
    summary = (
        f"Цитата в {signals.articles} статьях на {signals.domains} сайтах "
        f"({signals.mentions} упоминаний"
        f"{', ' + str(signals.members) + ' вариантов текста' if signals.members > 1 else ''}). "
        f"Где: {where}."
    )
    return ScoreResult(
        event_id=event.event_id,
        breakdown=result.breakdown,
        total=result.total,
        rationale=("Бесплатная оценка без LLM: " + "; ".join(result.notes))[:600],
        recommended_format="quote_card",
        target_action="save_quote",
        fact_check_required=True,
        fact_check_note="Автор и контекст цитаты определены не были: проверьте по источнику.",
        prompt_version=FREE_RANKER_VERSION,
        model_name="none",
        headline=event.original_text[:80],
        summary=summary[:600],
    )

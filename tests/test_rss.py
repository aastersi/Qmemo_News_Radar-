"""RSS/Atom collector: parsing, conditional requests, isolation, limits and address safety."""

import json
import sqlite3
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from qmemo_radar.application.collection import MultiSourceCollector
from qmemo_radar.application.filtering import FilterPolicy
from qmemo_radar.application.pipeline import PipelineThresholds, RadarPipeline
from qmemo_radar.bootstrap import SourceContext, build_collector, enabled_sources
from qmemo_radar.config import RadarSettings, SourcesConfig
from qmemo_radar.infrastructure.collectors.rss import Feed, RssCollector
from qmemo_radar.infrastructure.storage import SQLiteEventRepository

NOW = datetime(2026, 9, 16, 12, 0, tzinfo=UTC)

RSS = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:dc="http://purl.org/dc/elements/1.1/">
  <channel>
    <title>Wire</title>
    <language>en-us</language>
    <item>
      <title>Minister &amp; budget</title>
      <link>https://news.example/budget?utm_source=rss</link>
      <guid isPermaLink="false">wire-1</guid>
      <description><![CDATA[<p>The minister said the <b>budget</b> is final.</p>]]></description>
      <pubDate>Wed, 16 Sep 2026 11:30:00 +0200</pubDate>
      <dc:creator>Jane Reporter</dc:creator>
    </item>
    <item>
      <title>No guid here, the link identifies this story</title>
      <link>/relative/story</link>
    </item>
    <item>
      <description>An entry without any link cannot be stored</description>
    </item>
  </channel>
</rss>"""

ATOM = b"""<?xml version="1.0" encoding="utf-8"?>
<feed xmlns="http://www.w3.org/2005/Atom" xml:lang="de">
  <title>Blog</title>
  <entry>
    <id>tag:blog.example,2026:1</id>
    <title type="html">Release &lt;em&gt;notes&lt;/em&gt;</title>
    <link rel="replies" href="https://blog.example/1#comments"/>
    <link href="https://blog.example/1"/>
    <updated>2026-09-16T09:15:00Z</updated>
    <author><name>Max Muster</name></author>
    <summary>Summary of the release with enough words for the radar.</summary>
  </entry>
</feed>"""


def resolve_public(host: str, port: int) -> "object":
    async def resolved() -> list[str]:
        return ["127.0.0.1"] if host.endswith(".internal") else ["93.184.216.34"]

    return resolved()


class Web:
    def __init__(self, routes: dict[str, Callable[[httpx.Request], httpx.Response]]) -> None:
        self.routes = routes
        self.requests: list[httpx.Request] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        route = self.routes.get(str(request.url))
        return route(request) if route else httpx.Response(404)

    def collector(self, *feeds: tuple[str, str], max_bytes: int = 5_000_000) -> RssCollector:
        async def no_sleep(_: float) -> None:
            return None

        return RssCollector(
            httpx.AsyncClient(transport=httpx.MockTransport(self.handler)),
            [Feed(name, url) for name, url in feeds],
            max_bytes=max_bytes,
            resolve=resolve_public,  # type: ignore[arg-type]
            clock=lambda: NOW,
            sleep=no_sleep,
        )


def ok(body: bytes, **headers: str) -> Callable[[httpx.Request], httpx.Response]:
    return lambda request: httpx.Response(200, content=body, headers=headers)


def pipeline(collector: RssCollector, repository: SQLiteEventRepository) -> RadarPipeline:
    return RadarPipeline(
        collector=MultiSourceCollector({"rss": collector}),
        ranker=None,
        repository=repository,
        filter_policy=FilterPolicy(max_age=timedelta(days=36500)),
        thresholds=PipelineThresholds(),
    )


def rows(repository: SQLiteEventRepository) -> list[tuple[object, ...]]:
    with sqlite3.connect(repository._db_path) as db:
        return db.execute(
            "SELECT source_key, external_id, url, author_display_name, original_text, language,"
            " published_at FROM radar_events ORDER BY rowid"
        ).fetchall()


async def test_rss_and_atom_entries_become_items(repository: SQLiteEventRepository) -> None:
    web = Web({"https://wire.example/rss": ok(RSS), "https://blog.example/atom": ok(ATOM)})
    collector = web.collector(
        ("wire", "https://wire.example/rss"), ("blog", "https://blog.example/atom")
    )

    counters = await pipeline(collector, repository).run_once()

    assert (counters.inserted, counters.source_errors) == (3, 0)
    assert rows(repository) == [
        (
            "rss:wire",
            "wire:wire-1",
            "https://news.example/budget",
            "Jane Reporter",
            "Minister & budget\n\nThe minister said the budget is final.",
            "en-us",
            "2026-09-16T11:30:00+02:00",
        ),
        (
            "rss:wire",
            "wire:https://wire.example/relative/story",  # no guid: the canonical link
            "https://wire.example/relative/story",
            None,
            "No guid here, the link identifies this story",
            "en-us",
            NOW.isoformat(),  # no date: time of collection
        ),
        (
            "rss:blog",
            "blog:tag:blog.example,2026:1",
            "https://blog.example/1",
            "Max Muster",
            "Release notes\n\nSummary of the release with enough words for the radar.",
            "de",
            "2026-09-16T09:15:00+00:00",
        ),
    ]
    metrics = await repository.metrics_since(datetime(2000, 1, 1, tzinfo=UTC))
    assert metrics["rss:wire"] == {
        "clusters_created": 2,
        "collected": 2,
        "entries_accepted": 2,
        "entries_rejected": 1,
        "entries_seen": 3,
        "feeds_checked": 1,
        "inserted": 2,
    }


async def test_etag_and_last_modified_are_sent_back_and_304_is_a_quiet_success(
    repository: SQLiteEventRepository,
) -> None:
    def feed(request: httpx.Request) -> httpx.Response:
        if request.headers.get("if-none-match") == '"v1"':
            return httpx.Response(304)
        return httpx.Response(200, content=RSS, headers={"ETag": '"v1"'})

    modified = "Wed, 16 Sep 2026 10:00:00 GMT"

    def blog(request: httpx.Request) -> httpx.Response:
        if request.headers.get("if-modified-since") == modified:
            return httpx.Response(304)
        return httpx.Response(200, content=ATOM, headers={"Last-Modified": modified})

    web = Web({"https://wire.example/rss": feed, "https://blog.example/atom": blog})
    collector = web.collector(
        ("wire", "https://wire.example/rss"), ("blog", "https://blog.example/atom")
    )

    first = await pipeline(collector, repository).run_once()
    checkpoints = await repository.get_checkpoints()
    second = await pipeline(collector, repository).run_once()

    assert json.loads(checkpoints["rss:wire"]) == {"etag": '"v1"'}
    assert json.loads(checkpoints["rss:blog"]) == {"last_modified": modified}
    assert "if-none-match" not in web.requests[0].headers
    assert first.inserted == 3
    assert (second.collected, second.inserted, second.source_errors) == (0, 0, 0)
    assert await repository.get_checkpoints() == checkpoints  # a 304 keeps the validators
    metrics = await repository.metrics_since(datetime(2000, 1, 1, tzinfo=UTC))
    assert (metrics["rss:wire"]["feeds_checked"], metrics["rss:wire"]["feeds_304"]) == (2, 1)
    assert metrics["rss:blog"]["feeds_304"] == 1


async def test_a_repeated_feed_without_validators_stores_no_duplicates(
    repository: SQLiteEventRepository,
) -> None:
    web = Web({"https://wire.example/rss": ok(RSS)})
    collector = web.collector(("wire", "https://wire.example/rss"))

    await pipeline(collector, repository).run_once()
    again = await pipeline(collector, repository).run_once()

    assert (again.collected, again.inserted, again.duplicates) == (2, 0, 2)
    assert len(rows(repository)) == 2
    assert await repository.get_checkpoints() == {"rss:wire": "{}"}


@pytest.mark.parametrize(
    ("route", "code"),
    [
        (ok(b"<rss><channel><item><title>broken"), "rss_malformed_xml"),
        (ok(b"<html><body>not a feed</body></html>"), "rss_malformed_xml"),
        (
            ok(b'<!DOCTYPE r [<!ENTITY a "aaaaaaaaaa">]><rss><channel></channel></rss>'),
            "rss_malformed_xml",
        ),
        (ok(b"<rss>" + b" " * 2000 + b"</rss>"), "rss_response_too_large"),
        (lambda request: httpx.Response(500), "rss_server_error"),
        (lambda request: httpx.Response(410), "rss_http_410"),
    ],
)
async def test_one_bad_feed_does_not_block_another(
    repository: SQLiteEventRepository,
    route: Callable[[httpx.Request], httpx.Response],
    code: str,
) -> None:
    web = Web({"https://bad.example/rss": route, "https://wire.example/rss": ok(RSS)})
    collector = web.collector(
        ("bad", "https://bad.example/rss"), ("wire", "https://wire.example/rss"), max_bytes=1500
    )

    counters = await pipeline(collector, repository).run_once()

    assert (counters.inserted, counters.source_errors) == (2, 1)
    health = {item.source_key: item.last_error for item in await repository.source_health()}
    assert health["rss:bad"] == code and health["rss:wire"] is None
    assert "rss:bad" not in await repository.get_checkpoints()
    metrics = await repository.metrics_since(datetime(2000, 1, 1, tzinfo=UTC))
    assert metrics["rss:bad"] == {"feed_errors": 1, "feeds_checked": 1, "source_errors": 1}


async def test_redirects_are_followed_a_few_hops_but_never_to_private_addresses() -> None:
    web = Web(
        {
            "https://old.example/rss": lambda r: httpx.Response(301, headers={"Location": "/new"}),
            "https://old.example/new": ok(RSS),
            "https://sneaky.example/rss": lambda r: httpx.Response(
                302, headers={"Location": "http://metadata.internal/latest"}
            ),
            "https://loop.example/rss": lambda r: httpx.Response(
                302, headers={"Location": "https://loop.example/rss"}
            ),
        }
    )
    fetches = await web.collector(
        ("moved", "https://old.example/rss"),
        ("sneaky", "https://sneaky.example/rss"),
        ("loop", "https://loop.example/rss"),
        ("local", "http://admin.internal/feed"),
        ("ftp", "ftp://files.example/feed"),
    ).collect({})

    by_key = {fetch.source_key: fetch for fetch in fetches}
    assert len(by_key["rss:moved"].items) == 2
    assert by_key["rss:sneaky"].error_code == "rss_unsafe_address"
    assert by_key["rss:loop"].error_code == "rss_too_many_redirects"
    assert by_key["rss:local"].error_code == "rss_unsafe_address"
    assert by_key["rss:ftp"].error_code == "rss_unsafe_address"
    requested = [str(request.url) for request in web.requests]
    assert not any(".internal" in url or url.startswith("ftp") for url in requested)
    assert requested.count("https://loop.example/rss") == 6  # first request + 5 redirects


async def test_default_resolver_rejects_loopback_literals() -> None:
    web = Web({})
    collector = RssCollector(
        httpx.AsyncClient(transport=httpx.MockTransport(web.handler)),
        [Feed("local", "http://127.0.0.1:8080/feed"), Feed("v6", "http://[::1]/feed")],
        max_bytes=1000,
    )
    fetches = await collector.collect({})
    assert [fetch.error_code for fetch in fetches] == ["rss_unsafe_address"] * 2
    assert web.requests == []


def test_feeds_come_from_sources_yaml_and_need_no_key() -> None:
    sources = SourcesConfig.model_validate(
        {
            "rss": {
                "feeds": [
                    {"name": "wire", "url": "https://wire.example/rss"},
                    {"name": "off", "url": "https://off.example/rss", "enabled": False},
                ]
            }
        }
    )
    settings = RadarSettings(_env_file=None)  # type: ignore[call-arg]
    assert enabled_sources(settings, sources) == ["rss"]
    assert enabled_sources(settings, SourcesConfig()) == []
    context = SourceContext(settings, sources, x_client=None, free_http=httpx.AsyncClient())
    assert build_collector(context).names == ["rss"]
    with pytest.raises(ValueError, match="unique"):
        SourcesConfig.model_validate(
            {"rss": {"feeds": [{"name": "a", "url": "https://a.example"}] * 2}}
        )
    with pytest.raises(ValueError):
        SourcesConfig.model_validate({"rss": {"feeds": [{"name": "Bad Name", "url": "https://a"}]}})


async def test_entity_declarations_are_refused_in_any_encoding() -> None:
    # Regression: a byte search for `<!ENTITY` missed UTF-16 documents, and expat expanded a
    # 4.9 MB feed into a 326 MB title.
    bomb = (
        '<?xml version="1.0" encoding="UTF-16"?>'
        '<!DOCTYPE rss [<!ENTITY a "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa">'
        '<!ENTITY b "&a;&a;&a;&a;&a;&a;&a;&a;&a;&a;">]>'
        "<rss><channel><item><title>&b;</title><link>https://news.example/1</link></item>"
        "</channel></rss>"
    ).encode("utf-16")
    long_title = (
        "<rss><channel><item><title>" + "t" * 5000 + "</title>"
        "<link>https://news.example/2</link></item></channel></rss>"
    ).encode()
    web = Web({"https://bomb.example/rss": ok(bomb), "https://long.example/rss": ok(long_title)})

    fetches = await web.collector(
        ("bomb", "https://bomb.example/rss"), ("long", "https://long.example/rss")
    ).collect({})

    by_key = {fetch.source_key: fetch for fetch in fetches}
    assert by_key["rss:bomb"].error_code == "rss_malformed_xml"
    assert len(by_key["rss:long"].items[0].original_text) == 500


async def test_a_compressed_response_is_capped_while_it_is_decoded() -> None:
    # Regression: httpx decoded a whole received chunk before the size check, so ~50 KB of gzip
    # could expand to ~50 MB in memory before `rss_response_too_large`.
    import gzip
    import tracemalloc

    def encoded(body: bytes, encoding: str) -> Callable[[httpx.Request], httpx.Response]:
        # A raw stream, as from the network: httpx has not decoded it yet.
        headers = {"Content-Encoding": encoding}
        return lambda request: httpx.Response(200, stream=httpx.ByteStream(body), headers=headers)

    bomb = gzip.compress(b"<rss>" + b" " * (50 * 1024 * 1024))
    web = Web(
        {
            "https://bomb.example/rss": encoded(bomb, "gzip"),
            "https://wire.example/rss": encoded(gzip.compress(RSS), "gzip"),
            "https://br.example/rss": encoded(b"...", "br"),
        }
    )
    collector = web.collector(
        ("bomb", "https://bomb.example/rss"),
        ("wire", "https://wire.example/rss"),
        ("br", "https://br.example/rss"),
        max_bytes=1_000_000,
    )

    tracemalloc.start()
    fetches = await collector.collect({})
    peak = tracemalloc.get_traced_memory()[1]
    tracemalloc.stop()

    by_key = {fetch.source_key: fetch for fetch in fetches}
    assert by_key["rss:bomb"].error_code == "rss_response_too_large"
    assert len(by_key["rss:wire"].items) == 2  # gzip-encoded feeds still work
    assert by_key["rss:br"].error_code == "rss_unsupported_encoding"
    assert peak < 8 * 1024 * 1024  # never near the 50 MB the bomb expands to
    assert web.requests[0].headers["accept-encoding"] == "gzip, deflate"


async def test_truncated_several_member_and_raw_deflate_bodies_are_decoded_correctly() -> None:
    # Independent review: a gzip body cut off mid-stream was accepted as a shorter feed, the
    # second member of a multi-member gzip was dropped, and raw deflate was refused.
    import gzip
    import zlib

    def encoded(body: bytes, encoding: str) -> Callable[[httpx.Request], httpx.Response]:
        headers = {"Content-Encoding": encoding}
        return lambda request: httpx.Response(200, stream=httpx.ByteStream(body), headers=headers)

    whole = gzip.compress(RSS)
    half = len(RSS) // 2
    members = gzip.compress(RSS[:half]) + gzip.compress(RSS[half:])
    raw = zlib.compressobj(wbits=-15)
    deflated = raw.compress(RSS) + raw.flush()
    web = Web(
        {
            "https://cut.example/rss": encoded(whole[: len(whole) // 2], "gzip"),
            "https://members.example/rss": encoded(members, "gzip"),
            "https://raw.example/rss": encoded(deflated, "deflate"),
            "https://zlib.example/rss": encoded(zlib.compress(RSS), "deflate"),
        }
    )

    fetches = await web.collector(
        ("cut", "https://cut.example/rss"),
        ("members", "https://members.example/rss"),
        ("raw", "https://raw.example/rss"),
        ("zlib", "https://zlib.example/rss"),
    ).collect({})

    by_key = {fetch.source_key: fetch for fetch in fetches}
    assert by_key["rss:cut"].error_code == "rss_decode_error"
    assert [len(by_key[f"rss:{name}"].items) for name in ("members", "raw", "zlib")] == [2, 2, 2]

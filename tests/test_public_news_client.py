from __future__ import annotations

from datetime import datetime, timezone
from email.utils import format_datetime

from ingestion.public_news_client import PublicNewsClient


class FakeResponse:
    def __init__(self, content: bytes) -> None:
        self.content = content

    def raise_for_status(self) -> None:
        return None


class FakeSession:
    def __init__(self, xml: str) -> None:
        self.xml = xml
        self.urls: list[str] = []

    def get(self, url: str, timeout: int):
        self.urls.append(url)
        return FakeResponse(self.xml.encode("utf-8"))


def test_public_news_client_maps_policy_headline_to_affected_ticker():
    published = format_datetime(datetime.now(timezone.utc))
    xml = f"""
    <rss><channel>
      <item>
        <title>New CHIPS Act incentives boost semiconductor investment</title>
        <link>https://example.com/chips</link>
        <source>Policy Wire</source>
        <pubDate>{published}</pubDate>
        <description>Policy support is expected to benefit chipmakers.</description>
      </item>
    </channel></rss>
    """
    client = PublicNewsClient(session=FakeSession(xml), max_items_per_feed=5)

    headlines = client.fetch_headlines(
        tickers=["NVDA"],
        lookback_days=1,
        include_financial=False,
        include_political=True,
    )

    assert headlines
    assert any(
        headline.ticker == "NVDA"
        and headline.category == "political"
        and headline.industry == "semiconductors"
        and headline.topic == "semiconductor_export_controls"
        and "estimated policy impact is supportive" in (headline.summary or "")
        for headline in headlines
    )


def test_short_ticker_symbols_do_not_false_match_common_words():
    item = type(
        "Item",
        (),
        {
            "title": "Markets rally as consumers keep spending",
            "summary": "No company tickers are mentioned.",
        },
    )()

    assert PublicNewsClient._match_tickers(item, ["T", "C"]) == []

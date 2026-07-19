from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
import logging
from typing import Iterable
from urllib.parse import quote_plus
import xml.etree.ElementTree as ET

import requests

from config.news_topics import (
    POLITICAL_INDUSTRY_TOPICS,
    PUBLIC_FINANCIAL_FEEDS,
    PUBLIC_POLICY_FEEDS,
    TICKER_COMPANY_NAMES,
    classify_policy_impact,
    get_sector_for_ticker,
)
from models.raw_headline import RawHeadline


GOOGLE_NEWS_RSS_URL = "https://news.google.com/rss/search"
logger = logging.getLogger(__name__)


@dataclass(slots=True)
class ParsedFeedItem:
    title: str
    link: str
    source: str
    published_at_utc: datetime
    summary: str | None = None


class PublicNewsClient:
    """
    Fetches financial and political headlines from public RSS endpoints.

    This client intentionally avoids paid API credentials. It is not a market
    data entitlement layer; it is a pragmatic local feed collector for the
    Quicksilver demo pipeline.
    """

    def __init__(
        self,
        timeout_seconds: int = 20,
        max_items_per_feed: int = 8,
        session: requests.Session | None = None,
    ) -> None:
        self.timeout_seconds = timeout_seconds
        self.max_items_per_feed = max_items_per_feed
        self._session = session or requests.Session()
        if hasattr(self._session, "headers"):
            self._session.headers.update(
                {
                    "User-Agent": (
                        "Quicksilver local research pipeline "
                        "(contact: local-demo@example.com)"
                    )
                }
            )

    def fetch_headlines(
        self,
        tickers: Iterable[str],
        lookback_days: int,
        include_financial: bool = True,
        include_political: bool = True,
    ) -> list[RawHeadline]:
        ticker_list = [ticker.upper() for ticker in tickers]
        since = datetime.now(timezone.utc) - timedelta(days=lookback_days)
        headlines: list[RawHeadline] = []

        if include_financial:
            headlines.extend(self._fetch_company_search_headlines(ticker_list, since))
            headlines.extend(self._fetch_general_financial_headlines(ticker_list, since))

        if include_political:
            headlines.extend(self._fetch_political_headlines(set(ticker_list), since))
            headlines.extend(self._fetch_policy_feed_headlines(set(ticker_list), since))

        return self._dedupe_preserving_order(headlines)

    def _fetch_company_search_headlines(
        self,
        tickers: list[str],
        since: datetime,
    ) -> list[RawHeadline]:
        headlines: list[RawHeadline] = []

        for ticker in tickers:
            company_name = TICKER_COMPANY_NAMES.get(ticker, ticker)
            query = f'"{company_name}" OR "{ticker}" stock market news'
            url = self._google_news_url(query)

            for item in self._fetch_feed_items(url, fallback_source="Google News"):
                if item.published_at_utc < since:
                    continue

                headlines.append(
                    RawHeadline(
                        ticker=ticker,
                        headline=item.title,
                        source=item.source,
                        url=item.link,
                        published_at_utc=item.published_at_utc,
                        summary=item.summary,
                        category="financial",
                        topic="company_news",
                        industry=get_sector_for_ticker(ticker),
                    )
                )

        return headlines

    def _fetch_general_financial_headlines(
        self,
        tickers: list[str],
        since: datetime,
    ) -> list[RawHeadline]:
        headlines: list[RawHeadline] = []

        for feed in PUBLIC_FINANCIAL_FEEDS:
            for item in self._fetch_feed_items(
                str(feed["url"]),
                fallback_source=str(feed["name"]),
            ):
                if item.published_at_utc < since:
                    continue

                matched_tickers = self._match_tickers(item, tickers)
                for ticker in matched_tickers:
                    headlines.append(
                        RawHeadline(
                            ticker=ticker,
                            headline=item.title,
                            source=item.source,
                            url=item.link,
                            published_at_utc=item.published_at_utc,
                            summary=item.summary,
                            category="financial",
                            topic="market_news",
                            industry=get_sector_for_ticker(ticker),
                        )
                    )

        return headlines

    def _fetch_policy_feed_headlines(
        self,
        selected_tickers: set[str],
        since: datetime,
    ) -> list[RawHeadline]:
        headlines: list[RawHeadline] = []

        for feed in PUBLIC_POLICY_FEEDS:
            for item in self._fetch_feed_items(
                str(feed["url"]),
                fallback_source=str(feed["name"]),
            ):
                if item.published_at_utc < since:
                    continue

                for topic in self._matching_policy_topics(item):
                    affected_tickers = [
                        ticker
                        for ticker in topic["tickers"]
                        if str(ticker).upper() in selected_tickers
                    ]
                    for ticker in affected_tickers:
                        headlines.append(
                            RawHeadline(
                                ticker=str(ticker).upper(),
                                headline=item.title,
                                source=item.source,
                                url=item.link,
                                published_at_utc=item.published_at_utc,
                                summary=self._policy_summary(item, topic),
                                category="political",
                                topic=str(topic["topic"]),
                                industry=str(topic["industry"]),
                            )
                        )

        return headlines

    def _fetch_political_headlines(
        self,
        selected_tickers: set[str],
        since: datetime,
    ) -> list[RawHeadline]:
        headlines: list[RawHeadline] = []

        for topic in POLITICAL_INDUSTRY_TOPICS:
            affected_tickers = [
                ticker
                for ticker in topic["tickers"]
                if str(ticker).upper() in selected_tickers
            ]
            if not affected_tickers:
                continue

            url = self._google_news_url(str(topic["query"]))
            for item in self._fetch_feed_items(url, fallback_source="Google News Politics"):
                if item.published_at_utc < since:
                    continue

                for ticker in affected_tickers:
                    headlines.append(
                        RawHeadline(
                            ticker=str(ticker).upper(),
                            headline=item.title,
                            source=item.source,
                            url=item.link,
                            published_at_utc=item.published_at_utc,
                            summary=self._policy_summary(item, topic),
                            category="political",
                            topic=str(topic["topic"]),
                            industry=str(topic["industry"]),
                        )
                    )

        return headlines

    def _fetch_feed_items(
        self,
        url: str,
        fallback_source: str,
    ) -> list[ParsedFeedItem]:
        try:
            response = self._session.get(url, timeout=self.timeout_seconds)
            response.raise_for_status()
        except requests.RequestException as error:
            logger.warning("Skipping feed %s: %s", url, error)
            return []

        try:
            root = ET.fromstring(response.content)
        except ET.ParseError as error:
            logger.warning("Skipping malformed RSS feed %s: %s", url, error)
            return []

        parsed_items: list[ParsedFeedItem] = []
        for item_element in self._feed_entries(root)[: self.max_items_per_feed]:
            title = self._child_text(item_element, "title")
            published_at = self._parse_datetime(
                self._child_text(item_element, "pubDate")
                or self._child_text(item_element, "published")
                or self._child_text(item_element, "updated")
            )

            if not title or not published_at:
                continue

            parsed_items.append(
                ParsedFeedItem(
                    title=title,
                    link=(
                        self._child_text(item_element, "link")
                        or self._link_href(item_element)
                        or ""
                    ),
                    source=self._child_text(item_element, "source") or fallback_source,
                    published_at_utc=published_at,
                    summary=(
                        self._child_text(item_element, "description")
                        or self._child_text(item_element, "summary")
                        or self._child_text(item_element, "content")
                    ),
                )
            )

        return parsed_items

    @staticmethod
    def _google_news_url(query: str) -> str:
        return (
            f"{GOOGLE_NEWS_RSS_URL}?q={quote_plus(query)}"
            "&hl=en-US&gl=US&ceid=US:en"
        )

    @staticmethod
    def _child_text(element: ET.Element, child_name: str) -> str | None:
        for child in list(element):
            normalized_name = child.tag.rsplit("}", 1)[-1]
            if normalized_name == child_name and child.text:
                return " ".join(child.text.split())
        return None

    @staticmethod
    def _feed_entries(root: ET.Element) -> list[ET.Element]:
        rss_items = root.findall(".//item")
        if rss_items:
            return rss_items

        atom_entries = [
            element
            for element in root.iter()
            if element.tag.rsplit("}", 1)[-1] == "entry"
        ]
        return atom_entries

    @staticmethod
    def _link_href(element: ET.Element) -> str | None:
        for child in list(element):
            normalized_name = child.tag.rsplit("}", 1)[-1]
            if normalized_name == "link":
                href = child.attrib.get("href")
                if href:
                    return href
        return None

    @staticmethod
    def _parse_datetime(value: str | None) -> datetime | None:
        if not value:
            return None

        try:
            parsed = parsedate_to_datetime(value)
        except (TypeError, ValueError):
            try:
                parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                return None

        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    @staticmethod
    def _match_tickers(item: ParsedFeedItem, tickers: list[str]) -> list[str]:
        search_text = f"{item.title} {item.summary or ''}".lower()
        matched: list[str] = []

        for ticker in tickers:
            company_name = TICKER_COMPANY_NAMES.get(ticker, ticker)
            ticker_token = ticker.replace(".", "").lower()

            if (
                company_name.lower() in search_text
                or PublicNewsClient._ticker_symbol_mentioned(search_text, ticker)
                or (
                    len(ticker_token) > 2
                    and f" {ticker_token} " in f" {search_text} "
                )
            ):
                matched.append(ticker)

        return matched

    @staticmethod
    def _ticker_symbol_mentioned(search_text: str, ticker: str) -> bool:
        normalized_ticker = ticker.lower()
        compact_ticker = normalized_ticker.replace(".", "")
        if len(compact_ticker) <= 2:
            return f"${compact_ticker}" in search_text

        return (
            f" {normalized_ticker} " in f" {search_text} "
            or f"${compact_ticker}" in search_text
        )

    @staticmethod
    def _matching_policy_topics(item: ParsedFeedItem) -> list[dict[str, object]]:
        search_text = f"{item.title} {item.summary or ''}".casefold()
        matched_topics: list[dict[str, object]] = []

        for topic in POLITICAL_INDUSTRY_TOPICS:
            keywords = tuple(str(keyword).casefold() for keyword in topic.get("keywords", ()))
            if any(keyword in search_text for keyword in keywords):
                matched_topics.append(topic)

        return matched_topics

    @staticmethod
    def _policy_summary(item: ParsedFeedItem, topic: dict[str, object]) -> str | None:
        search_text = f"{item.title} {item.summary or ''}"
        impact = classify_policy_impact(search_text)
        summary = item.summary or ""
        policy_context = (
            f"Policy catalyst: {topic['topic']} for {topic['industry']}; "
            f"estimated policy impact is {impact}."
        )
        if summary:
            return f"{summary} {policy_context}"
        return policy_context

    @staticmethod
    def _dedupe_preserving_order(headlines: list[RawHeadline]) -> list[RawHeadline]:
        seen: set[tuple[str, str, str, str]] = set()
        unique: list[RawHeadline] = []

        for headline in headlines:
            key = (
                headline.ticker,
                headline.headline.casefold(),
                headline.url,
                headline.category,
            )
            if key in seen:
                continue

            seen.add(key)
            unique.append(headline)

        return unique

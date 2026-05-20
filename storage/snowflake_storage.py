import os
import snowflake.connector

from models.raw_headline import RawHeadline
from models.scored_headline import ScoredHeadline

class SnowflakeStorage:
    """
    Storage class for saving Quicksilver data into Snowflake.

    Snowflake is the cloud data warehouse.
    Python sends SQL commands to Snowflake through the Snowflake connector.
    """
    def __init__(self) -> None:
        self._connection = None
    
    def _connect(self):
        if self._connection is None:
            self._connection = snowflake.connector.connect(
                account=os.getenv("SNOWFLAKE_ACCOUNT"),
                user=os.getenv("SNOWFLAKE_USER"),
                password=os.getenv("SNOWFLAKE_PASSWORD"),
                role=os.getenv("SNOWFLAKE_ROLE"),
                warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                database=os.getenv("SNOWFLAKE_DATABASE"),
                schema=os.getenv("SNOWFLAKE_SCHEMA"),
            )

        return self._connection
    
    def create_tables(self) -> None:
        connection = self._connect()

        with connection.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS raw_headlines (
                    raw_headline_id INTEGER AUTOINCREMENT PRIMARY KEY,
                    ticker STRING NOT NULL,
                    headline STRING NOT NULL,
                    source STRING NOT NULL,
                    url STRING,
                    published_at_utc TIMESTAMP_TZ NOT NULL,
                    summary STRING,
                    inserted_at_utc TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
                )
                """
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS scored_headlines (
                    scored_headline_id INTEGER AUTOINCREMENT PRIMARY KEY,
                    ticker STRING NOT NULL,
                    headline STRING NOT NULL,
                    source STRING NOT NULL,
                    url STRING,
                    published_at_utc TIMESTAMP_TZ NOT NULL,
                    sentiment_label STRING NOT NULL,
                    positive_score FLOAT NOT NULL,
                    neutral_score FLOAT NOT NULL,
                    negative_score FLOAT NOT NULL,
                    compound_score FLOAT NOT NULL,
                    confidence FLOAT NOT NULL,
                    headline_age_hours FLOAT NOT NULL,
                    source_tier INTEGER NOT NULL,
                    summary STRING,
                    content_hash STRING,
                    inserted_at_utc TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
                )
                """
            )

    def save_raw_headline(self, headline: RawHeadline) -> None:
        """
        Insert one RawHeadline into the raw_headline table
        """
        connection = self._connect()

        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO raw_headlines (
                    ticker,
                    headline,
                    source,
                    url,
                    published_at_utc,
                    summary
                )
                VALUES (
                    %(ticker)s,
                    %(headline)s,
                    %(source)s,
                    %(url)s,
                    %(published_at_utc)s,
                    %(summary)s
                )
                """,
                {
                    "ticker": headline.ticker,
                    "headline": headline.headline,
                    "source": headline.source,
                    "url": headline.url,
                    "published_at_utc": headline.published_at_utc,
                    "summary": headline.summary,
                },
            )

    def save_raw_headlines(self, headlines: list[RawHeadline]) -> None:
        """
        Insert many RawHeadline objects.
        """
        for headline in headlines:
            self.save_raw_headline(headline)

    def save_scored_headline(self, headline: ScoredHeadline) -> None:
        """
        Insert one ScoredHeadline into the scored_headlines table.
        """
        connection = self._connect()

        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO scored_headlines (
                    ticker,
                    headline,
                    source,
                    url,
                    published_at_utc,
                    sentiment_label,
                    positive_score,
                    neutral_score,
                    negative_score,
                    compound_score,
                    confidence,
                    headline_age_hours,
                    source_tier,
                    summary,
                    content_hash
                )
                VALUES (
                    %(ticker)s,
                    %(headline)s,
                    %(source)s,
                    %(url)s,
                    %(published_at_utc)s,
                    %(sentiment_label)s,
                    %(positive_score)s,
                    %(neutral_score)s,
                    %(negative_score)s,
                    %(compound_score)s,
                    %(confidence)s,
                    %(headline_age_hours)s,
                    %(source_tier)s,
                    %(summary)s,
                    %(content_hash)s
                )
                """,
                {
                    "ticker": headline.ticker,
                    "headline": headline.headline,
                    "source": headline.source,
                    "url": headline.url,
                    "published_at_utc": headline.published_at_utc,
                    "sentiment_label": headline.sentiment_label,
                    "positive_score": headline.positive_score,
                    "neutral_score": headline.neutral_score,
                    "negative_score": headline.negative_score,
                    "compound_score": headline.compound_score,
                    "confidence": headline.confidence,
                    "headline_age_hours": headline.headline_age_hours,
                    "source_tier": headline.source_tier,
                    "summary": headline.summary,
                    "content_hash": headline.content_hash,
                },
            )

    def save_scored_headlines(self, headlines: list[ScoredHeadline]) -> None:
        """
        Insert many ScoredHeadline objects.
        """
        for headline in headlines:
            self.save_scored_headline(headline)

    def close(self) -> None:
        """
        Close the Snowflake connection when the pipeline is done.
        """
        if self._connection is not None:
            self._connection.close()
            self._connection = None
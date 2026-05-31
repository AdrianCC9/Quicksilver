import os
import re
import snowflake.connector

from models.raw_headline import RawHeadline
from models.scored_headline import ScoredHeadline
from transformations.headline_normalizer import HeadlineNormalizer

class SnowflakeStorage:
    """
    Storage class for saving Quicksilver data into Snowflake.

    Snowflake is the cloud data warehouse.
    Python sends SQL commands to Snowflake through the Snowflake connector.
    """
    def __init__(self) -> None:
        self._connection = None
        self._normalizer = HeadlineNormalizer()

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", identifier):
            raise ValueError(f"Unsafe Snowflake identifier: {identifier}")

        return f'"{identifier.upper()}"'
    
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
            warehouse = os.getenv("SNOWFLAKE_WAREHOUSE")
            if warehouse:
                quoted_warehouse = self._quote_identifier(warehouse)
                with self._connection.cursor() as cursor:
                    cursor.execute(
                        f"""
                        CREATE WAREHOUSE IF NOT EXISTS {quoted_warehouse}
                            WAREHOUSE_SIZE = XSMALL
                            AUTO_SUSPEND = 60
                            AUTO_RESUME = TRUE
                            INITIALLY_SUSPENDED = TRUE
                        """
                    )
                    cursor.execute(f"USE WAREHOUSE {quoted_warehouse}")

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
                    content_hash STRING,
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

            cursor.execute(
                """
                ALTER TABLE raw_headlines
                ADD COLUMN IF NOT EXISTS content_hash STRING
                """
            )

            cursor.execute(
                """
                ALTER TABLE scored_headlines
                ADD COLUMN IF NOT EXISTS content_hash STRING
                """
            )

    def _raw_content_hash(self, headline: RawHeadline) -> str:
        return self._normalizer.build_content_hash(headline)

    def _scored_content_hash(self, headline: ScoredHeadline) -> str:
        if headline.content_hash:
            return headline.content_hash

        raw_headline = RawHeadline(
            ticker=headline.ticker,
            headline=headline.headline,
            source=headline.source,
            url=headline.url,
            published_at_utc=headline.published_at_utc,
            summary=headline.summary,
        )
        return self._raw_content_hash(raw_headline)

    def save_raw_headline(self, headline: RawHeadline) -> None:
        """
        Insert one RawHeadline into the raw_headline table
        """
        connection = self._connect()
        content_hash = self._raw_content_hash(headline)

        with connection.cursor() as cursor:
            cursor.execute(
                """
                MERGE INTO raw_headlines AS target
                USING (
                    SELECT
                        %(ticker)s AS ticker,
                        %(headline)s AS headline,
                        %(source)s AS source,
                        %(url)s AS url,
                        %(published_at_utc)s AS published_at_utc,
                        %(summary)s AS summary,
                        %(content_hash)s AS content_hash
                ) AS source
                ON target.content_hash = source.content_hash
                WHEN NOT MATCHED THEN INSERT (
                    ticker,
                    headline,
                    source,
                    url,
                    published_at_utc,
                    summary,
                    content_hash
                ) VALUES (
                    source.ticker,
                    source.headline,
                    source.source,
                    source.url,
                    source.published_at_utc,
                    source.summary,
                    source.content_hash
                )
                """,
                {
                    "ticker": headline.ticker,
                    "headline": headline.headline,
                    "source": headline.source,
                    "url": headline.url,
                    "published_at_utc": headline.published_at_utc,
                    "summary": headline.summary,
                    "content_hash": content_hash,
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
        content_hash = self._scored_content_hash(headline)

        with connection.cursor() as cursor:
            cursor.execute(
                """
                MERGE INTO scored_headlines AS target
                USING (
                    SELECT
                        %(ticker)s AS ticker,
                        %(headline)s AS headline,
                        %(source)s AS source,
                        %(url)s AS url,
                        %(published_at_utc)s AS published_at_utc,
                        %(sentiment_label)s AS sentiment_label,
                        %(positive_score)s AS positive_score,
                        %(neutral_score)s AS neutral_score,
                        %(negative_score)s AS negative_score,
                        %(compound_score)s AS compound_score,
                        %(confidence)s AS confidence,
                        %(headline_age_hours)s AS headline_age_hours,
                        %(source_tier)s AS source_tier,
                        %(summary)s AS summary,
                        %(content_hash)s AS content_hash
                ) AS source
                ON target.content_hash = source.content_hash
                WHEN NOT MATCHED THEN INSERT (
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
                ) VALUES (
                    source.ticker,
                    source.headline,
                    source.source,
                    source.url,
                    source.published_at_utc,
                    source.sentiment_label,
                    source.positive_score,
                    source.neutral_score,
                    source.negative_score,
                    source.compound_score,
                    source.confidence,
                    source.headline_age_hours,
                    source.source_tier,
                    source.summary,
                    source.content_hash
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
                    "content_hash": content_hash,
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

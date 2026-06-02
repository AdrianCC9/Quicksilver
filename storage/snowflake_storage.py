import os
import re
import snowflake.connector
from uuid import uuid4

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

    def _temporary_stage_name(self, prefix: str) -> str:
        return self._quote_identifier(f"{prefix}_{uuid4().hex}")

    def save_raw_headline(self, headline: RawHeadline) -> None:
        """
        Insert one RawHeadline into the raw_headline table
        """
        self.save_raw_headlines([headline])

    def save_raw_headlines(self, headlines: list[RawHeadline]) -> None:
        """
        Insert many RawHeadline objects.
        """
        if not headlines:
            return

        connection = self._connect()
        stage_table = self._temporary_stage_name("qs_raw_stage")
        rows = [
            {
                "ticker": headline.ticker,
                "headline": headline.headline,
                "source": headline.source,
                "url": headline.url,
                "published_at_utc": headline.published_at_utc,
                "summary": headline.summary,
                "content_hash": self._raw_content_hash(headline),
            }
            for headline in headlines
        ]

        with connection.cursor() as cursor:
            try:
                cursor.execute(
                    f"""
                    CREATE TEMPORARY TABLE {stage_table} (
                        ticker STRING,
                        headline STRING,
                        source STRING,
                        url STRING,
                        published_at_utc TIMESTAMP_TZ,
                        summary STRING,
                        content_hash STRING
                    )
                    """
                )
                cursor.executemany(
                    f"""
                    INSERT INTO {stage_table} (
                        ticker,
                        headline,
                        source,
                        url,
                        published_at_utc,
                        summary,
                        content_hash
                    ) VALUES (
                        %(ticker)s,
                        %(headline)s,
                        %(source)s,
                        %(url)s,
                        %(published_at_utc)s,
                        %(summary)s,
                        %(content_hash)s
                    )
                    """,
                    rows,
                )
                cursor.execute(
                    f"""
                    MERGE INTO raw_headlines AS target
                    USING (
                        SELECT
                            ticker,
                            headline,
                            source,
                            url,
                            published_at_utc,
                            summary,
                            content_hash
                        FROM {stage_table}
                        QUALIFY ROW_NUMBER() OVER (
                            PARTITION BY content_hash
                            ORDER BY published_at_utc DESC
                        ) = 1
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
                    """
                )
            finally:
                cursor.execute(f"DROP TABLE IF EXISTS {stage_table}")

    def save_scored_headline(self, headline: ScoredHeadline) -> None:
        """
        Insert one ScoredHeadline into the scored_headlines table.
        """
        self.save_scored_headlines([headline])

    def save_scored_headlines(self, headlines: list[ScoredHeadline]) -> None:
        """
        Insert many ScoredHeadline objects.
        """
        if not headlines:
            return

        connection = self._connect()
        stage_table = self._temporary_stage_name("qs_scored_stage")
        rows = [
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
                "content_hash": self._scored_content_hash(headline),
            }
            for headline in headlines
        ]

        with connection.cursor() as cursor:
            try:
                cursor.execute(
                    f"""
                    CREATE TEMPORARY TABLE {stage_table} (
                        ticker STRING,
                        headline STRING,
                        source STRING,
                        url STRING,
                        published_at_utc TIMESTAMP_TZ,
                        sentiment_label STRING,
                        positive_score FLOAT,
                        neutral_score FLOAT,
                        negative_score FLOAT,
                        compound_score FLOAT,
                        confidence FLOAT,
                        headline_age_hours FLOAT,
                        source_tier INTEGER,
                        summary STRING,
                        content_hash STRING
                    )
                    """
                )
                cursor.executemany(
                    f"""
                    INSERT INTO {stage_table} (
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
                    rows,
                )
                cursor.execute(
                    f"""
                    MERGE INTO scored_headlines AS target
                    USING (
                        SELECT
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
                        FROM {stage_table}
                        QUALIFY ROW_NUMBER() OVER (
                            PARTITION BY content_hash
                            ORDER BY published_at_utc DESC
                        ) = 1
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
                    """
                )
            finally:
                cursor.execute(f"DROP TABLE IF EXISTS {stage_table}")

    def close(self) -> None:
        """
        Close the Snowflake connection when the pipeline is done.
        """
        if self._connection is not None:
            self._connection.close()
            self._connection = None

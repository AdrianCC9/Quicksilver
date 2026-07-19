from __future__ import annotations

from datetime import datetime, timezone
import os
from uuid import uuid4

import pytest

from models.raw_headline import RawHeadline
from storage.local_mysql_storage import LocalMySQLStorage


pytestmark = pytest.mark.skipif(
    os.getenv("RUN_MYSQL_INTEGRATION") != "true",
    reason="MySQL integration test is opt-in. Set RUN_MYSQL_INTEGRATION=true.",
)


def test_mysql_storage_creates_schema_and_persists_headline():
    database_url = os.environ["DATABASE_URL"]
    run_suffix = uuid4().hex[:8].upper()
    ticker = f"T{run_suffix[:6]}"
    headline_text = f"Quicksilver MySQL smoke headline {run_suffix}"

    storage = LocalMySQLStorage(database_url)
    try:
        storage.create_tables()
        storage.save_raw_headlines(
            [
                RawHeadline(
                    ticker=ticker,
                    headline=headline_text,
                    source="CI Smoke",
                    url=f"https://example.com/mysql-smoke/{run_suffix.lower()}",
                    published_at_utc=datetime.now(timezone.utc),
                    summary="MySQL integration smoke row.",
                )
            ]
        )

        rows = storage.fetch_dashboard_table("raw_headlines")
        matching = rows[
            (rows["ticker"] == ticker)
            & (rows["headline"] == headline_text)
        ]
        assert len(matching) == 1
    finally:
        storage.close()

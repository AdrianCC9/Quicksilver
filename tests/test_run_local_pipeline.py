from __future__ import annotations

from argparse import Namespace

from pipelines.run_local_pipeline import run_once
from storage.local_mysql_storage import LocalMySQLStorage


def test_run_local_pipeline_smoke_with_demo_seed(tmp_path):
    database_url = f"sqlite:///{tmp_path / 'pipeline.db'}"
    args = Namespace(
        database_url=database_url,
        tickers="AAPL,MSFT",
        large_cap_50=False,
        large_cap_100=False,
        max_tickers=2,
        lookback_days=1,
        include_finnhub=False,
        skip_public_news=True,
        skip_political_news=False,
        sentiment_backend="lexicon",
        skip_simulation=True,
        skip_evaluation=True,
        run_name="test",
        initial_cash_cad=5000,
        max_positions=2,
        cash_reserve_pct=0.05,
        seed_demo_if_empty=True,
        loop=False,
        interval_minutes=60,
        log_level="INFO",
    )

    summary = run_once(args)

    assert summary["raw_headlines_collected"] == 2
    assert summary["scored_headlines_saved_attempted"] == 2
    assert summary["insights_generated"] == 2

    storage = LocalMySQLStorage(database_url)
    assert len(storage.fetch_dashboard_table("raw_headlines")) == 2
    assert len(storage.fetch_dashboard_table("scored_headlines")) == 2
    assert len(storage.fetch_dashboard_table("insights")) == 2
    assert len(storage.fetch_dashboard_table("pipeline_run_logs")) == 1
    storage.close()

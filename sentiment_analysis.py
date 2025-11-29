import logging
from datetime import datetime, timedelta, timezone
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import Session
from create_db import DB_PATH, Feature

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

WINDOW_MINUTES = 60
ROLLING_LOOKBACK_WINDOWS = 24
WINDOW_NAME = f"{WINDOW_MINUTES}"

engine = create_engine(f"sqlite:///{DB_PATH}", echo=False, future=True)

# Compute rolling z-scores 
def compute_z_scores(series: pd.Series, window: int) -> pd.Series:

    rolling_mean = series.rolling(window, min_periods=5).mean()
    rolling_std = series.rolling(window, min_periods=5).std(ddof=0)

    z = (series - rolling_mean) / rolling_std
    z = z.replace([np.inf, -np.inf], 0.0).filna(0.0)

    return z
    
def build_features() -> None:
    """
    Main entrypoint: compute per-ticker rolling sentiment features and persist them.

    Steps:
    1. Load joined headlines + sentiment from SQLite into pandas.
    2. Bucket by (ticker, time window) and compute:
        - sent_mean: mean(sentiment_score) for the window.
        - headlines_n: number of headlines in the window.
    3. For each ticker, compute z-scores over a rolling window:
        - sent_z: z-score of sent_mean.
        - vol_z: z-score of headlines_n.
    4. Insert new rows into the FEATURES table, skipping any (ticker, window, ts_utc)
        that already exist.
    """
    now_utc = datetime.now(timezone.utc)
    lookback_minutes = WINDOW_MINUTES * (ROLLING_LOOKBACK_WINDOWS + 2)
    cutoff_dt = now_utc - timedelta(minutes=lookback_minutes)

    logging.info("Loading headlines + sentiment from DB...")
    with engine.connect() as conn:
        query = text(
            """
            SELECT
                h.id AS headline_id,
                h.ticker AS ticker,
                h.published_at_utc AS pubvlished_at_utc,
                s.score_pos AS score_pos,
                s.score_neu AS score_neu,
                s.score_neg AS score_neg
            FROM headlines AS h
            JOIN sentiment AS s
                ON s.headline_id = h.id
            """
        )
        df = pd.read_sql_query(query, conn)

        if df.empty:
            logging.info("No sentiment data found")
            return
        
        # Parse timestamps and filter to recent window
        
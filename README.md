# Quicksilver

### Project Description
Quicksilver is a Python project that continuously tracks stock-market sentiment by analyzing financial news headlines.
The system fetches news from the Finnhub API, cleans and normalizes the data, applies FinBERT (a finance-tuned language
model) for sentiment classification, and stores results in a SQLite database. Rolling metrics (averages, z-scores, volume)
are computed to detect significant sentiment changes, which can trigger alerts (Slack/email). A Streamlit dashboard
is included for exploring trends interactively.

### Tools & Libraries
- APIs & Data: Finnhub API, requests, tenacity
- Processing: pandas, numpy, datetime, hashlib
- Machine Learning: PyTorch (torch), Hugging Face Transformers (FinBERT)
- Database: SQLite, SQLAlchemy, Alembic
- Alerts: slack_sdk, smtplib
- Dashboard: Streamlit, matplotlib, seaborn
- Automation: schedule, logging

###Data & Retention Strategy
- Tickers:
  - Quicksilver is currently designed to monitor **up to ~200 tickers per day**.
  - A sensible default is to track the **top ~200 S&P 500 constituents by market capitalization**, since they are liquid and generate frequent, meaningful news flow.
  - The ticker universe is configurable (for example, via a simple config file or environment variable), so it can be swapped for a custom watchlist or sector-specific basket.

- Raw headline and sentiment retention:
  - Raw headlines and their associated sentiment inferences are stored in the `HEADLINES` and `SENTIMENT` tables.
  - To keep storage small and focused on recent context, Quicksilver keeps **only the last 3 days** of raw data.
  - A periodic cleanup job deletes older rows, e.g.:
    - `DELETE FROM HEADLINES WHERE published_at_utc < now() - 3 days;`
    - Associated `SENTIMENT` rows are removed by foreign-key cascade or a companion delete query.

- Aggregated features and alerts retention:
  - Aggregated time-window metrics (rolling sentiment averages, z-scores, headline volume, etc.) are stored in the `FEATURES` table.
  - Triggered events and rule hits are stored in the `ALERTS` table.
  - These are more compact and are kept longer for trend analysis and backtesting.
  - Quicksilver keeps **30 days** of `FEATURES` and `ALERTS`, removing anything older via a scheduled cleanup (for example, once per day).

- Storage footprint:
  - With 200 tickers, 3-day raw retention, and 30-day feature/alert retention, the expected SQLite database size remains comfortably within a few hundred megabytes on a typical laptop/desktop SSD.
  - This makes it practical to run Quicksilver locally without dedicated cloud storage.

### End-to-End Workflow
At a high level, Quicksilver runs as a scheduled pipeline that moves data through several states:

1. **Schedule tick (or cron trigger)**
   - A scheduler (`schedule` library in development, or OS-level cron/Task Scheduler in production) periodically invokes the main pipeline.
   - This entrypoint may call modules like `fetch_data.py`, `finbert_scoring.py`, and `normalize_store.py`.

2. **Fetch and normalize headlines**
   - Quicksilver calls the Finnhub API for the configured ticker universe (up to ~200 tickers).
   - Raw JSON responses are collected and converted into pandas DataFrames.
   - Timestamps are normalized to UTC, and basic cleaning/validation is applied.
   - A content hash (e.g., via `hashlib`) is computed per headline to deduplicate records.
   - New, unique headlines are inserted into the `HEADLINES` table via SQLAlchemy.

3. **Sentiment scoring with FinBERT**
   - Headlines that do not yet have sentiment are selected from the database.
   - A Hugging Face Transformers pipeline, backed by PyTorch and a FinBERT model, is used to compute:
     - Sentiment label (e.g., positive/neutral/negative).
     - Probability scores for each class.
     - Inference metadata (model version, inference time).
   - Results are written to the `SENTIMENT` table and linked back to each `HEADLINES` row.

4. **Feature computation and windowing**
   - Recent sentiment data per ticker is read from `HEADLINES` and `SENTIMENT`.
   - Using pandas, Quicksilver computes rolling metrics for each ticker and time window (for example, 5-minute, 1-hour, or 1-day windows), including:
     - Mean sentiment scores.
     - Z-scores for sentiment and headline volume.
     - Headline counts for the window.
   - A snapshot of these values is stored in the `FEATURES` table (one row per ticker per time window).

5. **Alert evaluation and notifications**
   - Quicksilver applies rule-based checks to `FEATURES` (for instance: “sentiment z-score < -2 and volume z-score > +2”).
   - For each feature row that meets the criteria and has not already fired a similar alert:
     - An entry is created in the `ALERTS` table with metadata about the rule, ticker, and timestamp.
     - A notification is sent via Slack (using `slack_sdk`) and/or email (via `smtplib` or provider SDK).

6. **Dashboard and exploration**
   - A Streamlit dashboard reads from `FEATURES` and `ALERTS`.
   - Users can filter by ticker, date range, or alert type to explore:
     - Time series of sentiment and volume.
     - Recent alerts and their context.
     - Short-term trends over the retained 30-day window.

7. **Retention and cleanup**
   - A scheduled maintenance job enforces the retention policies:
     - Raw `HEADLINES` and `SENTIMENT` older than 3 days are deleted.
     - Aggregated `FEATURES` and `ALERTS` older than 30 days are removed.
   - This keeps the SQLite database lean while preserving enough history to analyze recent behavior and validate alert logic.

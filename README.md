# Quicksilver

### Project Description

Quicksilver is a Python project that continuously tracks stock-market sentiment by analyzing financial news headlines.
The system fetches news from the Finnhub API, cleans and normalizes the data, applies FinBERT (a finance-tuned language
model) for sentiment classification, and stores results in a SQLite database. Rolling metrics (averages, z-scores, volume)
are computed to detect significant sentiment changes, which can trigger alerts (Slack/email). A Streamlit dashboard
is included for exploring trends interactively.

Quicksilver also includes an extension for **mock trading simulation**.
Using paper-trading APIs (such as Alpaca Markets), Quicksilver can automatically place *simulated trades* whenever an alert fires, track hypothetical performance, and show how well the strategy would have performed in real conditions—without risking real money.


### Tools & Libraries

* APIs & Data: Finnhub API, requests, tenacity
* Processing: pandas, numpy, datetime, hashlib
* Machine Learning: PyTorch (torch), Hugging Face Transformers (FinBERT)
* Database: SQLite, SQLAlchemy, Alembic
* Alerts: slack_sdk, smtplib
* Dashboard: Streamlit, matplotlib, seaborn
* Automation: schedule, logging
* **Mock Trading (Optional)**: Alpaca Markets Paper Trading API, alpaca-trade-api Python SDK


### Data & Retention Strategy

* **Tickers**

  * Quicksilver is designed to monitor **up to ~200 tickers per day**.
  * A practical default is the **top ~200 S&P 500 companies by market cap**.
  * The ticker list is fully configurable.

* **Raw headline and sentiment retention**

  * Raw headline data and per-headline FinBERT sentiment are stored in `HEADLINES` and `SENTIMENT`.
  * To keep the DB lightweight, Quicksilver retains **only the last 3 days** of raw data.
  * A scheduled cleanup job removes older entries.

* **Aggregated feature retention**

  * Rolling metrics (sent_mean, sent_z, vol_z) are stored in `FEATURES`.
  * Alerts and signal events are stored in `ALERTS`.
  * These compact records are kept for **30 days**.

* **Storage footprint**

  * With 200 tickers, 3-day raw retention, and 30-day feature retention, SQLite stays under a few hundred MB.


### End-to-End Workflow

At a high level, Quicksilver runs as a scheduled pipeline that moves data through several states:

1. **Schedule tick**

   * A scheduler triggers the main pipeline every N minutes.

2. **Fetch and normalize headlines**

   * Retrieve news from Finnhub.
   * Normalize timestamps, dedupe using content hashes, and store in `HEADLINES`.

3. **Sentiment scoring with FinBERT**

   * Unscored headlines are passed through FinBERT.
   * Results are stored in `SENTIMENT`.

4. **Feature computation**

   * Sentiment scores are aggregated into fixed time windows (e.g., 1h).
   * Compute:

     * Average sentiment
     * Sentiment z-score
     * Volume z-score
     * Headlines per window
   * Store each snapshot into `FEATURES`.

5. **Alert evaluation**

   * Rule-based triggers (e.g., “sent_z < −2 && vol_z > +2”).
   * Matches are stored in `ALERTS` and optionally sent to Slack/email.

6. **Dashboard**

   * Streamlit reads from `FEATURES` and `ALERTS`.
   * Interactive charts show sentiment trends, volume spikes, and triggered alerts.

7. **Retention and cleanup**

   * Automatically remove old raw data (3 days).
   * Remove old features/alerts (30 days).


### Mock Trading & Trade Simulation (Optional Quicksilver Extension)

Quicksilver can also act as a **signal-to-trade simulator**, showing how profitable the strategy *would have been* if trades were executed in real markets.

#### How it works

1. When an alert fires, Quicksilver sends a simulated trade to a **paper trading broker**, such as:

   * **Alpaca Markets Paper Trading API** (recommended)

     * Free, real-time, no real money required
     * Clean REST API + Python SDK
     * Handles hypothetical fills, PnL, equity, and positions automatically

2. Quicksilver logs trades and performance into a `TRADES` table (optionally stored locally in SQLite).

3. Streamlit visualizes:

   * Entry/exit points
   * Open positions
   * PnL and equity curve
   * How the algorithm would have performed over time

#### Why Alpaca is recommended

* 100% free paper trading environment
* Realistic market execution logic
* Full Python API (`alpaca-trade-api`)
* Simple order placement:

  ```python
  api.submit_order(
      symbol="AAPL",
      qty=10,
      side="buy",
      type="market",
      time_in_force="day"
  )
  ```

#### How Quicksilver connects to Alpaca

1. Install the SDK:

   ```bash
   pip install alpaca-trade-api
   ```
2. Add API keys to a `.env` file.
3. `trade_simulator.py` listens for new alerts in the `ALERTS` table.
4. Each alert triggers a **paper trade** (long/short) through Alpaca.
5. Quicksilver periodically reads account/broker data to evaluate hypothetical performance.
6. Streamlit displays simulated earnings, equity, and trade logs.

#### Purpose of the simulator

* Evaluate whether Quicksilver’s sentiment signals are **profitable**.
* Test strategies safely **without risking real capital**.
* Analyze performance over days, weeks, or months using automated alerts + real price data.

This turns Quicksilver into a full end-to-end experimental trading research platform:
**sentiment → metrics → alerts → simulated trades → profit evaluation**.
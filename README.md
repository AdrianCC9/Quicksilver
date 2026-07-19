# Quicksilver

## Description

Quicksilver is a local-first stock sentiment analytics pipeline with an optional cloud/streaming path. The default local workflow runs without Snowflake credentials: it ingests fresh financial and political/policy headlines from public RSS feeds, scores each headline, stores raw and processed data in MySQL, creates ticker-level insights, tests those insights in a mock exchange using a $5,000 CAD paper portfolio, and stores real-market performance evaluations for later review.

The project is designed to demonstrate data engineering judgment across ingestion, storage, scoring, analytics modeling, dashboarding, health checks, containerization, and reproducible testing. Kafka, Snowflake, dbt, Airflow, and FinBERT are retained as the optional cloud-style path; the MySQL/Streamlit path is the polished default demo.

## Tools and Libraries

### Core Language
- **Python**  
  Main programming language used for ingestion, streaming producers/consumers, NLP inference, data loading, alerting, and dashboard logic.

### Data Source
- **Finnhub API**  
  Optional source for fresh financial news headlines when `FINNHUB_ENABLED=true` and a key is configured.
- **Public RSS / Google News RSS**  
  Default no-credential source for financial headlines and political/policy headlines that can affect industries such as semiconductors, energy, banking, healthcare, EVs, retail, telecom, cybersecurity, logistics, travel, and big tech.
- **requests**  
  Python HTTP library used to call Finnhub, public RSS feeds, optional quote providers, and no-key price data sources.
- **Polygon / Alpha Vantage**
  Optional premium quote providers. Set `POLYGON_API_KEY` and/or `ALPHA_VANTAGE_API_KEY`, then control preference with `PRICE_PROVIDER_ORDER`. Without keys, Quicksilver falls back to Yahoo/Stooq and then deterministic synthetic prices for offline continuity.
- **Hardcoded 50-ticker equity watchlist**  
  The canonical ticker universe lives in `config/watchlist.py` and is used by ingestion, backfill, Airflow, and local scripts. Set `ADDITIONAL_TICKERS` to append names, or set `USE_CUSTOM_TICKERS=true` with `DEFAULT_TICKERS` to replace the canonical list.
- **Expanded 100-ticker universe**  
  The local runner supports `--large-cap-100`, adding more U.S. and Canadian large-cap names for broader market coverage.

### Streaming Infrastructure
- **Apache Kafka**  
  Real-time event streaming platform used to move headlines through the pipeline as messages rather than relying on a simple scheduled batch script.
- **confluent-kafka**  
  Python client library used to publish headlines to Kafka topics and consume them downstream for scoring and storage.

### NLP / Machine Learning
- **Hugging Face Transformers**  
  Used to load and run the FinBERT sentiment model.
- **PyTorch**  
  Backend deep learning framework that powers FinBERT inference.
- **FinBERT**  
  Finance-specific transformer model used to classify headlines as positive, neutral, or negative.

### Data Handling
- **pandas**  
  Used for light preprocessing, timestamp normalization, validation, debugging, and local inspection of data before or after warehouse loading.

### Local Storage / Warehousing
- **MySQL**  
  Default local database used to store raw headline data, sentiment outputs, generated insights, price quotes, mock trades, positions, portfolio snapshots, pipeline run logs, health alerts, report runs, and insight performance evaluations.
- **Alembic**
  Explicit migration path for local MySQL schema changes.
- **Snowflake**  
  Optional cloud data warehouse path retained for raw headline data, sentiment outputs, and analytics-ready tables.
- **snowflake-connector-python**  
  Python connector for loading data into and querying Snowflake.

### Transformation Layer
- **dbt (Data Build Tool)**  
  Used to transform raw Snowflake tables into clean analytical models such as rolling sentiment averages, z-scores, headline volume summaries, volume-weighted sentiment indexes, and anomaly flags.

### Orchestration
- **Apache Airflow**  
  Workflow orchestrator used to schedule and manage the dependencies between ingestion, streaming, scoring, loading, transformation, and alerting tasks.

### Operational Dashboard
- **Streamlit**  
  Used to build a real-time operational dashboard for monitoring live pipeline activity, exploring sentiment outputs, inspecting generated insights, and reviewing the mock portfolio equity curve.
- **Matplotlib**  
  Used for foundational plotting inside the Streamlit dashboard.
- **Seaborn**  
  Used for cleaner statistical visualizations and trend-focused charts.

### Alerts / Notifications
- **Slack Incoming Webhooks via requests**  
  Sends alert notifications to Slack when sentiment or local pipeline health rules are triggered.
- **smtplib** or an email provider SDK  
  Sends email alerts for important sentiment events.

### Reliability / Operations
- **Docker**  
  Containerizes MySQL, local pipeline workers, Kafka, Airflow, and the Streamlit dashboard so the environment runs consistently across machines.
- **tenacity**  
  Adds retry logic for API calls and other transient failures.
- **logging**  
  Used for structured pipeline logs and debugging.

## General Pipeline Process

Quicksilver follows a layered local pipeline by default:

1. **News Ingestion**
   - Python reads public financial RSS feeds, ticker-specific Google News RSS searches, and political/policy topic searches.
   - Political headlines are mapped to affected industries and tickers before scoring.

2. **Local Storage**
   - Raw normalized headlines are written to MySQL with content-hash deduplication.

3. **Sentiment Scoring**
   - The default local scorer is a deterministic lexicon scorer for fast no-download runs.
   - FinBERT is still available with `SENTIMENT_BACKEND=finbert`.

4. **Insight Generation**
   - Recent scored headlines are aggregated into ticker-level positive, neutral, or negative signals.
   - Political/policy catalysts are retained in the insight rationale.
   - The engine stores sector, source diversity, sentiment momentum, consensus, risk, opportunity, recommendation, and confidence grade.

5. **Mock Exchange**
   - The simulator starts with `$5,000 CAD`.
   - It buys the strongest positive signals, liquidates positions with negative signals, and marks positions to no-key Stooq quotes with deterministic fallback prices.

6. **Dashboard**
   - Streamlit reads MySQL and shows sentiment trends, signal summaries, political headline counts, open positions, trades, portfolio equity curve, run logs, health alerts, generated report history, and real-market performance summaries.

7. **Health + Reporting**
   - Each local pipeline run persists a health status for low headline coverage, low insight coverage, high synthetic quote usage, invalid portfolio equity, and failures.
   - Weekly reports export CSV, Markdown, and PDF summaries under `./.data/reports`.

The Snowflake/Kafka/dbt/Airflow path remains available for the original cloud-style workflow.

## High-Level Architecture

```
Public financial + political RSS feeds
→ Local headline normalizer
→ MySQL raw_headlines
→ Sentiment scorer
→ MySQL scored_headlines
→ Insight engine
→ Mock stock exchange / CAD portfolio
→ Real-market insight evaluator
→ Health checks + weekly reports
→ Streamlit dashboard
```

Optional legacy/cloud path:

```
Finnhub API
→ Kafka Producer
→ Kafka Topic
→ FinBERT Scoring Consumer
→ Snowflake Raw Tables
→ dbt Models
→ Analytics Tables / Alert Models
→ Streamlit Operational Dashboard
```

## Local MySQL Pipeline

Copy the sample environment file if you want local overrides:

```bash
cp .env.example .env
```

Create the local virtual environment for Makefile commands:

```bash
python -m venv .venv
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -r requirements/dev.txt
```

For the fastest interview/demo view, run the generated-data dashboard:

```bash
make demo
```

Then open:

```text
http://127.0.0.1:8502
```

For the full local pipeline and dashboard:

```bash
make local-up
```

Start the local automation and dashboard:

```bash
docker compose --profile local-pipeline --profile dashboard up -d --build mysql streamlit local-pipeline
```

This starts MySQL, runs the local pipeline immediately over the expanded 100-ticker universe, then repeats every 60 minutes. It stores all raw headlines, scored headlines, insights, mock trades, positions, snapshots, pipeline logs, and performance evaluations in `./.data/mysql`.

Open the dashboard at:

```text
http://localhost:8501
```

The first dashboard tab is designed for interviews: it shows the followed
stocks, latest available prices, the hottest tickers from current article
sentiment, and the latest pipeline status. The remaining tabs keep the deeper
trend, portfolio, pipeline, and raw data views available without crowding the
main presentation screen.

For a one-off local run against an already-running MySQL service:

```bash
docker compose run --rm local-pipeline python pipelines/run_local_pipeline.py --large-cap-50
```

For the expanded universe:

```bash
docker compose run --rm local-pipeline python pipelines/run_local_pipeline.py --large-cap-100
```

For a no-network smoke run that seeds deterministic sample headlines:

```bash
docker compose run --rm local-pipeline python pipelines/run_local_pipeline.py \
  --tickers AAPL,MSFT,NVDA \
  --skip-public-news \
  --seed-demo-if-empty
```

To reset only the local paper portfolio while keeping headlines, insights, and evaluations:

```bash
docker compose run --rm local-pipeline python scripts/reset_local_portfolio.py
```

Apply database migrations:

```bash
docker compose --profile maintenance run --rm migrate
```

Run a one-off local health check:

```bash
docker compose --profile maintenance run --rm health-check
```

Health alerts are always persisted to MySQL. External Slack/email delivery is disabled
by default; opt in with `LOCAL_HEALTH_NOTIFICATIONS_ENABLED=true` plus the relevant
Slack or SMTP settings when you intentionally want notifications sent.

Generate the weekly CSV/Markdown/PDF report:

```bash
docker compose --profile reports run --rm weekly-report
```

Reports are written under:

```text
./.data/reports
```

Run the full automated test suite in a lightweight, reproducible container:

```bash
docker compose --profile test run --rm test
```

For local development checks:

```bash
make lint
make test
make compile
```

Use optional premium quote data by exporting one or both keys before starting the pipeline:

```bash
export POLYGON_API_KEY=...
export ALPHA_VANTAGE_API_KEY=...
export PRICE_PROVIDER_ORDER=polygon,alpha_vantage,yahoo,stooq
docker compose --profile local-pipeline --profile dashboard up -d --build mysql streamlit local-pipeline
```

## Data Modes

Quicksilver separates three data modes so portfolio/demo results are not confused with live market evidence:

- **Demo mode** uses deterministic generated dashboard data. Start it with `make demo` or `DASHBOARD_DEMO_MODE=true`.
- **Local live mode** stores real RSS headlines, sentiment scores, quotes, trades, health checks, and reports in MySQL.
- **Synthetic quote fallback** is used only when live quote providers are unavailable. Synthetic quotes keep the app usable offline, but the dashboard and health checks label this path separately from real-market evaluations.

## Optional Snowflake Path

MySQL is the default backend because it runs locally without a subscription.
The Snowflake implementation is retained separately in `storage/snowflake_storage.py`,
`storage/setup_snowflake.py`, `dbt/`, and the Kafka/Airflow pipeline files. To
switch back later, install `requirements/full.txt`, set `STORAGE_BACKEND=snowflake`
with the `SNOWFLAKE_*` environment variables, run `python storage/setup_snowflake.py`,
and use the existing dbt/Kafka/Airflow commands.

## Viewing Results After A Week

Leave the local pipeline running with:

```bash
docker compose --profile local-pipeline --profile dashboard up -d --build mysql streamlit local-pipeline
```

In a week, open `http://localhost:8501` and check:

- **Mock Portfolio** for total equity, cash, open positions, recent trades, and cumulative return from the initial `$5,000 CAD`.
- **Real-Market Performance** for evaluated insights, real quote eval count, win rate, and average forward return.
- **Recent Insight Evaluations** for per-ticker forward return, direction correctness, data source, and whether the signal has matured.
- **Pipeline Run Log** for each automation run, counts collected, insights generated, trades executed, and any errors.
- **Operational Health** for low coverage warnings, pipeline failures, stale runs, and synthetic quote usage.
- **Weekly Reports** for generated report paths and report periods.
- **Signal Summary** for the latest generated insights, including political/policy headline counts, recommendations, source diversity, momentum, risk, opportunity, and rationales.
- **Sentiment Trend** and **Market Sentiment Index** for whether the model's signals strengthened or weakened over the week.

If the containers were stopped, restart them with the same compose command. The MySQL data persists under `./.data/mysql`.

## Project Layout

- `analytics/` - insight generation and local dashboard aggregation.
- `config/` - settings, watchlists, public news feeds, and policy topic maps.
- `dashboard/` - Streamlit UI plus data-source adapters for demo, local MySQL, and Snowflake modes.
- `docker/` - full and local Docker images.
- `ingestion/` - credentialed Finnhub client and no-credential public RSS client.
- `migrations/` - explicit Alembic migrations for local MySQL schema history.
- `models/` - dataclasses passed between ingestion, scoring, insights, and storage.
- `pipelines/` - executable ingestion, scoring, backfill, and local end-to-end runners.
- `requirements/` - dependency pins split into full and lightweight local stacks.
- `scripts/` - local maintenance helpers.
- `simulation/` - price providers, mock exchange, and insight evaluation.
- `storage/` - local schema definitions, MySQL persistence, and retained Snowflake implementation.
- `.env.example` - documented local defaults and optional provider credentials.
- `Makefile` - short commands for tests, linting, demo mode, and Docker workflows.
- `pyproject.toml` - pytest and lint configuration.

## Historical Backfill

The live local pipeline handles current ingestion, while `pipelines/backfill_historical_headlines.py` can create a historical corpus needed for rolling metrics and resume-scale validation. It defaults to the configured storage backend, which is local MySQL unless `STORAGE_BACKEND=snowflake` is set.

Example two-year backfill:

```bash
python pipelines/backfill_historical_headlines.py \
  --large-cap-50 \
  --from-date 2024-05-30 \
  --to-date 2026-05-30 \
  --create-tables \
  --score-and-save
```

Use `--publish-kafka` when the historical events should also be replayed through Kafka. Use `--storage-backend snowflake` for the retained Snowflake implementation. Use `--plan-only` to inspect the request plan without calling Finnhub or storage.

## Resume Claim Audit

The dbt model `pipeline_claim_audit` validates the scale-oriented claims from warehouse data:

- `tracked_ticker_count >= 50`
- `coverage_days >= 730`
- `max_scored_headlines_in_one_day >= 500`

The dashboard exposes this audit table alongside ticker-level sentiment trends and the market-level volume-weighted sentiment index.

## Project Goal

Quicksilver is designed as a portfolio-grade system that demonstrates practical skills in:
- local-first data pipeline design
- public API/RSS ingestion and normalization
- sentiment scoring with lightweight and FinBERT-backed options
- relational schema design, migrations, and upserts
- analytics engineering with pandas and optional dbt/Snowflake marts
- operational health checks, reports, and CI
- Dockerized dashboard and worker workflows
- financial signal evaluation and mock portfolio analysis
- optional Kafka/Airflow orchestration for the cloud-style path

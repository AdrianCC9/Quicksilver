# Quicksilver

Quicksilver is a personal project that uses financial news sentiment to create simple stock recommendations for S&P 500 companies. It collects headlines, scores them with FinBERT and Finnhub sentiment data, stores the results, and shows the latest recommendations in a Streamlit dashboard.

The project can run with either MySQL or Snowflake. MySQL is recommended for now because it runs locally and does not require Snowflake credentials. The Snowflake path is included for running the same idea with a warehouse, dbt models, Kafka, and Airflow once credentials are available.

Basic workflow:

1. Load the S&P 500 ticker list.
2. Collect recent headlines for those tickers.
3. Score the headlines with FinBERT and Finnhub sentiment data.
4. Save raw headlines, scored headlines, and recommendations.
5. Backtest the recommendations with about six months of historical data.
6. Display recommendations, headlines, backtest results, and pipeline status in Streamlit.

## Run with MySQL

This is the recommended way to run Quicksilver right now.

Install the Python dependencies:

```bash
python -m venv .venv
.venv/bin/python -m pip install --upgrade pip
.venv/bin/python -m pip install -r requirements/dev.txt
```

Run the tests:

```bash
make test
```

Open the dashboard with demo data:

```bash
make demo
```

Then open:

```text
http://127.0.0.1:8502
```

Run the local MySQL version with Docker:

```bash
make local-up
```

Then open:

```text
http://localhost:8501
```

Stop the local Docker version:

```bash
make local-down
```

## Run with Snowflake

The Snowflake version requires Snowflake credentials, so MySQL is easier to use until those are available.

To use Snowflake later:

1. Install the full requirements.
2. Add the `SNOWFLAKE_*` values to `.env`.
3. Set `STORAGE_BACKEND=snowflake`.
4. Run the Snowflake setup script.
5. Use the Kafka, dbt, and Airflow files for the warehouse pipeline.

Main Snowflake-related folders:

- `storage/` - Snowflake connection and loading code.
- `dbt/` - SQL models for transformed warehouse tables.
- `streaming/` - Kafka producer and consumer code.
- `orchestration/` - Airflow DAG.

## Main tools

- Python
- SQL
- MySQL
- Snowflake
- Kafka
- dbt
- Airflow
- Docker
- Streamlit
- pandas
- FinBERT
- Finnhub

## Main folders

- `config/` - Settings and S&P 500 ticker list.
- `ingestion/` - Headline collection.
- `sentiment/` - Sentiment scoring.
- `analytics/` - Recommendation logic.
- `pipelines/` - Pipeline and backfill scripts.
- `simulation/` - Paper trading backtest.
- `dashboard/` - Streamlit dashboard.
- `tests/` - Project tests.

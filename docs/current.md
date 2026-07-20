# Current Architecture

Quicksilver's maintained default path is the local MySQL and Streamlit workflow:

```text
Public RSS / Google News RSS
-> Raw headline normalization
-> MySQL raw_headlines
-> Lexicon or FinBERT sentiment scoring
-> MySQL scored_headlines
-> Ticker-level insight generation
-> Mock exchange and price-provider evaluation
-> Health checks and weekly reports
-> Streamlit dashboard
```

The optional cloud/streaming path remains available for demonstrating Kafka,
Snowflake, dbt, Airflow, and FinBERT integration:

```text
Finnhub API
-> Kafka producer
-> Kafka topic
-> FinBERT scoring consumer
-> Snowflake raw/scored tables
-> dbt marts
-> Streamlit dashboard and alert checks
```

See `docs/package_diagram.md` and `docs/class_diagram.md` for diagrams that
match the current code layout.

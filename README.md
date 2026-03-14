# Quicksilver

## Description

Quicksilver is a real-time stock sentiment intelligence pipeline built to simulate the modern data engineering stack used in financial institutions. The system ingests fresh company news headlines from the Finnhub API, streams them through Apache Kafka, scores each headline with FinBERT using Hugging Face Transformers and PyTorch, stores raw and processed data in Snowflake, transforms that data into analytics-ready models with dbt, orchestrates the full workflow with Apache Airflow, and surfaces insights through two reporting layers: a Power BI reporting suite connected directly to Snowflake for business-facing analytics, and a Streamlit operational dashboard for real-time monitoring. When unusual sentiment shifts are detected, Quicksilver can send automated Slack and email alerts.

The goal of the project is not just to classify headline sentiment, but to demonstrate an end-to-end production-style data platform that combines streaming, machine learning inference, cloud warehousing, analytics engineering, orchestration, containerization, business intelligence reporting, and financial data analysis in a single system.

## Tools and Libraries

### Core Language
- **Python**  
  Main programming language used for ingestion, streaming producers/consumers, NLP inference, data loading, alerting, and dashboard logic.

### Data Source
- **Finnhub API**  
  Provides fresh financial news headlines for tracked equity tickers.
- **requests**  
  Python HTTP library used to call the Finnhub API.

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

### Cloud Storage / Warehousing
- **Snowflake**  
  Cloud data warehouse used to store raw headline data, sentiment outputs, and analytics-ready tables. Also serves as the direct data source for the Power BI reporting layer via native connector.
- **snowflake-connector-python**  
  Python connector for loading data into and querying Snowflake.

### Transformation Layer
- **dbt (Data Build Tool)**  
  Used to transform raw Snowflake tables into clean analytical models such as rolling sentiment averages, z-scores, headline volume summaries, and anomaly flags. These dbt-modeled tables are consumed directly by Power BI for reporting.

### Orchestration
- **Apache Airflow**  
  Workflow orchestrator used to schedule and manage the dependencies between ingestion, streaming, scoring, loading, transformation, and alerting tasks.

### Business Intelligence / Reporting
- **Power BI**  
  Connected to Snowflake via the native Snowflake connector to deliver a multi-page business intelligence reporting suite on top of the dbt-transformed analytics tables. Provides stakeholder-facing dashboards for sentiment trend analysis, per-ticker signal history, and alert summaries. DAX measures are used for calculated metrics including rolling average sentiment and signal frequency by ticker. The report is published to Power BI Service for live sharing and scheduled data refresh.
  - **Page 1 — Sentiment Overview:** Line charts showing rolling average sentiment scores over time across the tracked equity watchlist, with ticker-level slicers for filtering.
  - **Page 2 — Per-Ticker Signal History:** Table and bar chart views of recent headlines, their FinBERT classification scores, and z-score threshold breach flags per ticker.
  - **Page 3 — Alert Summary:** Card visuals showing current signal counts, negative/positive sentiment breakdowns, and conditional formatting that flags tickers in an active alert state.

### Operational Dashboard
- **Streamlit**  
  Used to build a real-time operational dashboard for monitoring live pipeline activity, exploring raw sentiment outputs, and inspecting alert events as they occur.
- **Matplotlib**  
  Used for foundational plotting inside the Streamlit dashboard.
- **Seaborn**  
  Used for cleaner statistical visualizations and trend-focused charts.

### Alerts / Notifications
- **slack_sdk**  
  Sends alert notifications to Slack when rules are triggered.
- **smtplib** or an email provider SDK  
  Sends email alerts for important sentiment events.

### Reliability / Operations
- **Docker**  
  Containerizes the project so the full environment runs consistently across machines.
- **tenacity**  
  Adds retry logic for API calls and other transient failures.
- **logging**  
  Used for structured pipeline logs and debugging.

## General Pipeline Process

Quicksilver follows a layered pipeline:

1. **News Ingestion**
   - Python calls the Finnhub API for selected stock tickers.
   - Each returned headline is treated as a new event.

2. **Streaming**
   - Headlines are published into a Kafka topic.
   - Kafka acts as the message backbone of the system, allowing downstream components to consume events reliably.

3. **Sentiment Scoring**
   - A Kafka consumer reads each headline.
   - FinBERT classifies the headline sentiment and outputs sentiment probabilities or labels.

4. **Raw Storage**
   - The scored headline data is written into Snowflake raw tables.
   - This layer preserves the original input and model output for traceability.

5. **Transformation**
   - dbt models run inside Snowflake to transform raw data into analytics tables.
   - These models compute features such as rolling averages, z-scores, headline counts, and anomaly indicators by ticker and time window.
   - The resulting analytics tables serve as the data source for both the Power BI reporting layer and the Streamlit dashboard.

6. **Orchestration**
   - Airflow coordinates the full pipeline and ensures each task runs in the correct order.
   - It also handles scheduling, retries, and task monitoring.

7. **Business Intelligence Reporting (Power BI)**
   - Power BI connects to Snowflake directly via native connector, reading from the dbt analytics tables.
   - Multi-page dashboards provide stakeholder-facing views of sentiment trends, per-ticker signal history, and alert summaries.
   - DAX measures compute additional business metrics on top of the warehouse data without modifying the underlying models.
   - The report is refreshed on a schedule via Power BI Service and can be shared as a live link.

8. **Operational Dashboard and Alerts**
   - Streamlit reads the transformed analytics tables from Snowflake and provides a real-time operational view of pipeline activity and sentiment outputs.
   - If alert conditions are met, Slack or email notifications are triggered automatically.

## High-Level Architecture

```
Finnhub API
→ Kafka Producer
→ Kafka Topic
→ FinBERT Scoring Consumer
→ Snowflake Raw Tables
→ dbt Models
→ Analytics Tables / Alert Models
→ Power BI Reporting Suite (Snowflake native connector → Power BI Service)
→ Streamlit Operational Dashboard
→ Slack / Email Alerts
```

## Reporting Layer: Power BI vs. Streamlit

Quicksilver uses two complementary reporting surfaces that serve different audiences:

| | Power BI | Streamlit |
|---|---|---|
| **Audience** | Business stakeholders, analysts | Engineers, pipeline operators |
| **Data connection** | Snowflake native connector (scheduled refresh) | Live Snowflake queries |
| **Primary use** | Trend analysis, signal history, alert summaries | Real-time pipeline monitoring |
| **Interactivity** | Slicers, filters, drillthrough, DAX measures | Live charts, raw data inspection |
| **Distribution** | Published to Power BI Service, shareable link | Local or deployed app |

## Project Goal

Quicksilver is designed as a portfolio-grade system that demonstrates practical skills in:
- real-time data streaming
- NLP inference
- cloud data warehousing
- analytics engineering
- orchestration
- containerization
- business intelligence reporting and stakeholder-facing dashboard delivery
- financial data analysis
# Quicksilver

## Description

Quicksilver is a real-time stock sentiment intelligence pipeline built to simulate the modern data engineering stack used in financial institutions. The system ingests fresh company news headlines from the Finnhub API, streams them through Apache Kafka, scores each headline with FinBERT using Hugging Face Transformers and PyTorch, stores raw and processed data in Snowflake, transforms that data into analytics-ready models with dbt, orchestrates the full workflow with Apache Airflow, and surfaces insights through a Streamlit dashboard. When unusual sentiment shifts are detected, Quicksilver can send automated Slack and email alerts.

The goal of the project is not just to classify headline sentiment, but to demonstrate an end-to-end production-style data platform that combines streaming, machine learning inference, cloud warehousing, analytics engineering, orchestration, containerization, and financial data analysis in a single system.

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
  Cloud data warehouse used to store raw headline data, sentiment outputs, and analytics-ready tables.
- **snowflake-connector-python**  
  Python connector for loading data into and querying Snowflake.

### Transformation Layer
- **dbt (Data Build Tool)**  
  Used to transform raw Snowflake tables into clean analytical models such as rolling sentiment averages, z-scores, headline volume summaries, and anomaly flags.

### Orchestration
- **Apache Airflow**  
  Workflow orchestrator used to schedule and manage the dependencies between ingestion, streaming, scoring, loading, transformation, and alerting tasks.

### Dashboard / Visualization
- **Streamlit**  
  Used to build an interactive dashboard for exploring sentiment trends, ticker-level analytics, and alert events.
- **Matplotlib**
  Used for foundational plotting inside the dashboard.
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

6. **Orchestration**
   - Airflow coordinates the full pipeline and ensures each task runs in the correct order.
   - It also handles scheduling, retries, and task monitoring.

7. **Dashboard and Alerts**
   - Streamlit reads the transformed analytics tables from Snowflake and visualizes sentiment trends.
   - If alert conditions are met, Slack or email notifications are triggered automatically.

## High-Level Architecture

Finnhub API  
→ Kafka Producer  
→ Kafka Topic  
→ FinBERT Scoring Consumer  
→ Snowflake Raw Tables  
→ dbt Models  
→ Analytics Tables / Alert Models  
→ Streamlit Dashboard + Slack/Email Alerts

## Project Goal

Quicksilver is designed as a portfolio-grade system that demonstrates practical skills in:
- real-time data streaming
- NLP inference
- cloud data warehousing
- analytics engineering
- orchestration
- containerization
- financial data analysis
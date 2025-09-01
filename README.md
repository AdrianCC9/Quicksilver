Quicksilver
============

Project Description
-------------------
Quicksilver is a Python project that continuously tracks stock-market sentiment by analyzing financial news headlines.
The system fetches news from the Finnhub API, cleans and normalizes the data, applies FinBERT (a finance-tuned language
model) for sentiment classification, and stores results in a SQLite database. Rolling metrics (averages, z-scores, volume)
are computed to detect significant sentiment changes, which can trigger alerts (Slack/email). A Streamlit dashboard
is included for exploring trends interactively.

Tools & Libraries
-----------------
- APIs & Data: Finnhub API, requests, tenacity
- Processing: pandas, numpy, datetime, hashlib
- Machine Learning: PyTorch (torch), Hugging Face Transformers (FinBERT)
- Database: SQLite, SQLAlchemy, Alembic
- Alerts: slack_sdk, smtplib
- Dashboard: Streamlit, matplotlib, seaborn
- Automation: schedule, logging


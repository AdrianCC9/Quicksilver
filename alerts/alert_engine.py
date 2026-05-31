import os
import smtplib
from email.message import EmailMessage
from typing import Any

import pandas as pd
import requests
import snowflake.connector
from dotenv import load_dotenv


load_dotenv()


REQUIRED_ENV_VARS = [
    "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_USER",
    "SNOWFLAKE_PASSWORD",
    "SNOWFLAKE_ROLE",
    "SNOWFLAKE_WAREHOUSE",
    "SNOWFLAKE_DATABASE",
    "SNOWFLAKE_SCHEMA",
]

def get_missing_env_vars() -> list[str]:
    return [name for name in REQUIRED_ENV_VARS if not os.getenv(name)]


def get_snowflake_connection():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )


def get_signal_view_name() -> str:
    database = os.getenv("SNOWFLAKE_DATABASE")
    schema = os.getenv("SNOWFLAKE_SCHEMA")
    return f"{database}.{schema}.sentiment_signal_summary"


def format_number(value, digits: int = 3) -> str:
    if pd.isna(value):
        return "n/a"

    return f"{value:.{digits}f}"


def load_active_signals() -> pd.DataFrame:
    query = f"""
        select
            ticker,
            sentiment_date,
            headline_count,
            avg_compound_score,
            rolling_7_day_avg_compound_score,
            rolling_7_day_volume_weighted_sentiment_index,
            compound_score_zscore,
            is_positive_sentiment_signal,
            is_negative_sentiment_signal,
            is_positive_zscore_anomaly,
            is_negative_zscore_anomaly
        from {get_signal_view_name()}
        where sentiment_date = (
            select max(sentiment_date)
            from {get_signal_view_name()}
        )
        and (
            is_positive_sentiment_signal = true
            or is_negative_sentiment_signal = true
        )
        order by ticker
    """

    connection = get_snowflake_connection()

    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            dataframe = cursor.fetch_pandas_all()
    finally:
        connection.close()

    dataframe.columns = [column.lower() for column in dataframe.columns]
    return dataframe

def format_alert_message(signals: pd.DataFrame) -> str:
    if signals.empty:
        return "Quicksilver alert check completed. No active sentiment signals found."

    lines = [
        "Quicksilver sentiment alert",
        "",
        f"Active signals found: {len(signals)}",
        "",
    ]

    for _, row in signals.iterrows():
        signal_type = (
            "positive"
            if row["is_positive_sentiment_signal"]
            else "negative"
        )

        lines.append(
            "- "
            f"{row['ticker']} | "
            f"{signal_type.upper()} | "
            f"date={row['sentiment_date']} | "
            f"headlines={row['headline_count']} | "
            f"avg_score={format_number(row['avg_compound_score'])} | "
            f"rolling_avg={format_number(row['rolling_7_day_avg_compound_score'])} | "
            f"volume_weighted={format_number(row['rolling_7_day_volume_weighted_sentiment_index'])} | "
            f"zscore={format_number(row['compound_score_zscore'])}"
        )

    return "\n".join(lines)


def format_pipeline_failure_message(context: dict[str, Any]) -> str:
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown"
    task_id = context.get("task_instance").task_id if context.get("task_instance") else "unknown"
    run_id = context.get("run_id", "unknown")
    exception = context.get("exception")

    return "\n".join(
        [
            "Quicksilver pipeline failure",
            "",
            f"dag={dag_id}",
            f"task={task_id}",
            f"run_id={run_id}",
            f"exception={exception}",
        ]
    )


def send_pipeline_failure_alert(context: dict[str, Any]) -> None:
    message = format_pipeline_failure_message(context)
    print(message)
    send_slack_alert(message)
    send_email_alert(message)

def send_slack_alert(message: str) -> None:
    if os.getenv("SLACK_ENABLED", "false").lower() != "true":
        return

    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url:
        raise ValueError("SLACK_ENABLED is true, but SLACK_WEBHOOK_URL is missing.")

    response = requests.post(
        webhook_url,
        json={"text": message},
        timeout=15,
    )
    response.raise_for_status()

def send_email_alert(message: str) -> None:
    if os.getenv("EMAIL_ENABLED", "false").lower() != "true":
        return

    recipients = [
        email.strip()
        for email in os.getenv("ALERT_EMAIL_TO", "").split(",")
        if email.strip()
    ]

    if not recipients:
        raise ValueError("EMAIL_ENABLED is true, but ALERT_EMAIL_TO is missing.")

    email_message = EmailMessage()
    email_message["Subject"] = "Quicksilver sentiment alert"
    email_message["From"] = os.getenv("SMTP_USERNAME")
    email_message["To"] = ", ".join(recipients)
    email_message.set_content(message)

    with smtplib.SMTP(
        os.getenv("SMTP_HOST"),
        int(os.getenv("SMTP_PORT", "587")),
    ) as server:
        server.starttls()
        server.login(
            os.getenv("SMTP_USERNAME"),
            os.getenv("SMTP_PASSWORD"),
        )
        server.send_message(email_message)

def main() -> None:
    missing_env_vars = get_missing_env_vars()
    if missing_env_vars:
        raise ValueError(
            "Missing required environment variables: "
            + ", ".join(missing_env_vars)
        )

    signals = load_active_signals()
    message = format_alert_message(signals)

    print(message)

    if not signals.empty:
        send_slack_alert(message)
        send_email_alert(message)


if __name__ == "__main__":
    main()

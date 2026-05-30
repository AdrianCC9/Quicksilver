import os
import re
from datetime import date

import altair as alt
import pandas as pd
import snowflake.connector
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(
    page_title="Quicksilver Dashboard",
    page_icon="QS",
    layout="wide",
)

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

def quote_identifier(identifier: str) -> str:
    """
    Snowflake table/view names cannot be passed as normal SQL parameters,
    so we validate them before putting them into a query.
    """
    if not re.fullmatch()
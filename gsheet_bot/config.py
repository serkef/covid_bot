""" Main configuration and settings """

import os
from pathlib import Path

from dotenv import find_dotenv, load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

load_dotenv(find_dotenv())

RESOURCES = Path(__file__).parent / "resources"

# Log settings
APP_LOGS = Path(os.environ["APP_LOGS"])
APP_LOGLEVEL = os.getenv("APP_LOGLEVEL", "INFO")

# Secrets
TWITTER_CONSUMER_KEY = os.environ["CONSUMER_KEY"]
TWITTER_CONSUMER_KEY_SECRET = os.environ["CONSUMER_KEY_SECRET"]
TWITTER_ACCESS_TOKEN = os.environ["ACCESS_TOKEN"]
TWITTER_ACCESS_TOKEN_SECRET = os.environ["ACCESS_TOKEN_SECRET"]

# Gsheet
GSHEET_API_SCOPES = os.environ["GSHEET_API_SCOPES"]
GSHEET_API_SERVICE_ACCOUNT_FILE = os.environ["GSHEET_API_SERVICE_ACCOUNT_FILE"]
GSHEET_POLLING_INTERVAL_SEC = int(os.getenv("GSHEET_POLLING_INTERVAL_SEC", "60"))
GSHEET_SPREADSHEET_ID = os.environ["GSHEET_SPREADSHEET_ID"]
GSHEET_SHEET_DAILY_NAME = os.environ["GSHEET_SHEET_DAILY_NAME"]
GSHEET_SHEET_LIVE_NAME = os.environ["GSHEET_SHEET_LIVE_NAME"]

# slack
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

# Local DB
DB_PATH = Path(os.environ["DB_PATH"])
DB_CREATE_RAW_DAILY_TABLE = Path(__file__).parent / "sql" / "raw_daily_data.sql"
DB_CREATE_RAW_HOME_TABLE = Path(__file__).parent / "sql" / "raw_home_data.sql"
DB_CREATE_LATEST_DAILY_TABLE = Path(__file__).parent / "sql" / "latest_daily_data.sql"
DB_CREATE_LATEST_HOME_TABLE = Path(__file__).parent / "sql" / "latest_home_data.sql"
DB_CREATE_LATEST_POSTS_TABLE = Path(__file__).parent / "sql" / "post_daily_data.sql"
DB_GET_LATEST_UPDATES = Path(__file__).parent / "sql" / "query_daily_data.sql"
DB_GET_TOTAL_COUNTS = Path(__file__).parent / "sql" / "total_daily_data.sql"
DB_INSERT_RAW_DAILY_DATA = Path(__file__).parent / "sql" / "insert_raw_daily_data.sql"
DB_INSERT_RAW_HOME_DATA = Path(__file__).parent / "sql" / "insert_raw_home_data.sql"


STATUS_HEADER = "#BREAKING latest #COVIDー19 #CoronavirusPandemic update"
STATUS_FOOTER = "Visit 📊covid2019.app for the latest updates"

POST_TWITTER = os.getenv("POST_TWITTER", "false") == "true"
POST_SLACK = os.getenv("POST_SLACK", "false") == "true"


def build_db_session():
    """ Creates an sqlalchemy session for db"""

    # db setup
    connection_string = "postgresql://{user}:{psw}@{host}/{db}".format(
        host=os.getenv("DB_HOST", ""),
        db=os.getenv("DB_NAME", ""),
        user=os.getenv("DB_USER", ""),
        psw=os.getenv("DB_PASSWORD", ""),
    )
    engine = create_engine(connection_string, pool_pre_ping=True)
    session = sessionmaker()
    session.configure(bind=engine)
    return session


DbSession = build_db_session()

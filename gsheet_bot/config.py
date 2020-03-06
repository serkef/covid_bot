""" Main configuration and settings """

import os
import sqlite3
from pathlib import Path

from dotenv import find_dotenv, load_dotenv

load_dotenv(find_dotenv())

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
GSHEET_SHEET_NAME = os.environ["GSHEET_SHEET_NAME"]

# slack
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

# Local DB
DB_PATH = Path(os.environ["DB_PATH"])
DB_CREATE_RAW_TABLE = (
    "CREATE TABLE IF NOT EXISTS raw_data ("
    "id INTEGER PRIMARY KEY AUTOINCREMENT, "
    "ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
    "rec_dt DATE NOT NULL, "
    "rec_territory TEXT NOT NULL, "
    "rec_value NUMERIC"
    ")"
)
DB_CREATE_LATEST_DAILY_TABLE = (
    "CREATE TABLE IF NOT EXISTS latest_daily ("
    "rec_dt DATE NOT NULL, "
    "rec_territory TEXT NOT NULL, "
    "rec_value NUMERIC, "
    "PRIMARY KEY (rec_dt, rec_territory)"
    ")"
)
DB_CREATE_POSTS = (
    "CREATE TABLE IF NOT EXISTS posts ("
    "id INTEGER PRIMARY KEY AUTOINCREMENT, "
    "ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
    "rec_dt DATE NOT NULL, "
    "rec_territory TEXT NOT NULL, "
    "rec_value NUMERIC "
    ")"
)
DB_GET_LATEST_UPDATES = """
    SELECT 
    latest_daily.rec_dt, latest_daily.rec_territory, latest_daily.rec_value
    
    FROM 
    latest_daily AS latest_daily    
    LEFT JOIN (
        SELECT 
            max(ts) AS ts, 
            rec_dt, 
            rec_territory, 
            max(rec_value) AS rec_value
        FROM posts
        GROUP BY rec_dt, rec_territory
    ) AS latest_posts
    ON latest_daily.rec_territory = latest_posts.rec_territory
    AND latest_daily.rec_dt = latest_posts.rec_dt
    
    WHERE 
    latest_daily.rec_value > 0  
    AND (latest_posts.rec_value IS NULL OR latest_posts.rec_value < latest_daily.rec_value)  
    AND (latest_posts.rec_value IS NULL OR strftime('%s','now') - strftime('%s', latest_posts.ts) > 3600) 
    AND julianday('now')- julianday(latest_daily.rec_dt) <= 1 
    AND 0 < julianday('now')- julianday(latest_daily.rec_dt)
"""

DB_GET_TOTAL_COUNTS = """
    SELECT 
        SUM(latest_daily.rec_value)
    FROM
        latest_daily
    WHERE
        latest_daily.rec_territory = "{territory}"
"""

DB_CONNECTION = sqlite3.connect(f"{DB_PATH}")

STATUS_TEMPLATE = """
#BREAKING latest #COVID2019 update
{message}

Visit 📊covid2019.app for the latest updates
👉 Follow @covid2019app & fill in the form https://forms.gle/XM4RzKk3QU7CtHQq9 to join our team
"""

POST_TWITTER = os.getenv("POST_TWITTER", "false") == "true"
POST_SLACK = os.getenv("POST_SLACK", "false") == "true"

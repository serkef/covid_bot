""" custom utilities """
import json
import logging
import sqlite3
from logging.handlers import TimedRotatingFileHandler

import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build
import tweepy

from .config import (
    APP_LOGS,
    TWITTER_CONSUMER_KEY,
    TWITTER_CONSUMER_KEY_SECRET,
    TWITTER_ACCESS_TOKEN,
    TWITTER_ACCESS_TOKEN_SECRET,
    ENV,
    DB_PATH,
    DB_CREATE_RAW_TABLE,
    DB_CREATE_POSTS,
    DB_CREATE_LATEST_DAILY_TABLE,
    SLACK_WEBHOOK_URL,
    STATUS_TEMPLATE,
    GSHEET_API_SERVICE_ACCOUNT_FILE,
    GSHEET_API_SCOPES,
)


def create_tables():
    conn = sqlite3.connect(f"{DB_PATH}")
    cursor = conn.cursor()
    cursor.execute(DB_CREATE_RAW_TABLE)
    cursor.execute(DB_CREATE_LATEST_DAILY_TABLE)
    cursor.execute(DB_CREATE_POSTS)
    conn.commit()
    conn.close()


def slack_status(status):
    logger = logging.getLogger(f"{__name__}.slack_status")
    logger.info(f"Posting status {status!r}")

    requests.post(
        SLACK_WEBHOOK_URL,
        data=json.dumps({"text": status}),
        headers={"Content-Type": "application/json"},
    )


def create_status(day, territory, value):
    return STATUS_TEMPLATE.format(day=day, territory=territory, value=value)


def tweet_status(status):
    logger = logging.getLogger(f"{__name__}.tweet_status")
    logger.info(f"Posting status {status!r}")

    if ENV == "production":
        auth = tweepy.OAuthHandler(TWITTER_CONSUMER_KEY, TWITTER_CONSUMER_KEY_SECRET)
        auth.set_access_token(TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_TOKEN_SECRET)
        api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
        api.update_status(status)
    else:
        logger.info("Skipping - not in production")


def get_gsheet_api():
    """ Initializes Google API. Taken from quickstart example """

    credentials = service_account.Credentials.from_service_account_file(
        filename=GSHEET_API_SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )

    service = build("sheets", "v4", credentials=credentials)

    # Call the Sheets API
    return service.spreadsheets()


def set_logging(loglevel: [int, str] = "INFO"):
    """ Sets logging handlers """

    fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    formatter = logging.Formatter(fmt)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    stream_handler.setLevel(loglevel)

    APP_LOGS.mkdir(parents=True, exist_ok=True)
    log_path = f"{APP_LOGS / 'gsheet-bot.log'}"
    file_handler = TimedRotatingFileHandler(
        filename=log_path, when="midnight", encoding="utf-8"
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(loglevel)

    errlog_path = f"{APP_LOGS / 'gsheet-bot.err'}"
    err_file_handler = TimedRotatingFileHandler(
        filename=errlog_path, when="midnight", encoding="utf-8"
    )
    err_file_handler.setFormatter(formatter)
    err_file_handler.setLevel(logging.WARNING)

    logger = logging.getLogger()
    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)
    logger.addHandler(err_file_handler)
    logger.setLevel(loglevel)

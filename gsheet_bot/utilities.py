""" custom utilities """
import json
import logging
from logging.handlers import TimedRotatingFileHandler

import requests
import tweepy
import flag
from country_converter import convert
from tweepy import TweepError

from .config import (
    APP_LOGS,
    TWITTER_CONSUMER_KEY,
    TWITTER_CONSUMER_KEY_SECRET,
    TWITTER_ACCESS_TOKEN,
    TWITTER_ACCESS_TOKEN_SECRET,
    POST_SLACK,
    POST_TWITTER,
    DB_CREATE_RAW_DAILY_TABLE,
    DB_CREATE_LATEST_POSTS_TABLE,
    DB_CREATE_LATEST_DAILY_TABLE,
    SLACK_WEBHOOK_URL,
    build_db_session,
    DB_CREATE_RAW_HOME_TABLE,
    DB_CREATE_LATEST_HOME_TABLE,
    RESOURCES,
    STATUS_FOOTER,
    STATUS_HEADER,
)


def read_file(filepath):
    with open(filepath, "r") as fin:
        return fin.read()


def create_tables():
    sess = build_db_session()
    cursor = sess()
    cursor.execute(read_file(DB_CREATE_RAW_DAILY_TABLE))
    cursor.execute(read_file(DB_CREATE_LATEST_DAILY_TABLE))
    cursor.execute(read_file(DB_CREATE_LATEST_POSTS_TABLE))
    cursor.execute(read_file(DB_CREATE_RAW_HOME_TABLE))
    cursor.execute(read_file(DB_CREATE_LATEST_HOME_TABLE))
    cursor.commit()
    cursor.close()


def slack_status(status):
    logger = logging.getLogger(f"{__name__}.slack_status")
    logger.info(f"Posting status {status!r}")

    if POST_SLACK:
        requests.post(
            SLACK_WEBHOOK_URL,
            data=json.dumps({"text": status}),
            headers={"Content-Type": "application/json"},
        )
    else:
        logger.info("Will not post to slack - disabled")


def get_hashtag_country(territory):
    hashword = "".join(c for c in territory.lower() if c.isalpha())
    if hashword:
        return f"#{hashword}"
    return territory


def get_emoji_country(territory):
    try:
        return flag.flag(convert(names=[territory], to="ISO2"))
    except ValueError:
        return ""


def create_status(total, day, country, value):

    country = get_hashtag_country(country)
    emoji = get_emoji_country(country)

    if value == 1:
        msg = f"A new case reported today in {emoji} {country}. Raises total to {total:,d}."
        if total == 1:
            msg = f"First case reported in {emoji} {country}."
    else:
        msg = f"{value:,d} new cases reported today in {emoji} {country}. Raises total to {total:,d}."
        if value == total:
            msg = f"First {value:,d} cases reported in {emoji} {country}."

    if len(msg) > 240:
        msg = msg.replace("#CoronavirusPandemic ", "")

    status = STATUS_HEADER + "\n\n" + msg + "\n\n" + STATUS_FOOTER
    return status


def tweet_status(status):
    logger = logging.getLogger(f"{__name__}.tweet_status")
    logger.info(f"Posting status {status!r}")
    media_filepath = RESOURCES / "BREAKING-COVID2019APP.jpg"

    if POST_TWITTER:
        auth = tweepy.OAuthHandler(TWITTER_CONSUMER_KEY, TWITTER_CONSUMER_KEY_SECRET)
        auth.set_access_token(TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_TOKEN_SECRET)
        api = tweepy.API(auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
        try:
            api.update_with_media(str(media_filepath), status=status)
        except TweepError:
            logger.error(f"Cannot post message {status}", exc_info=True)
    else:
        logger.info("Will not post to twitter - disabled")


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

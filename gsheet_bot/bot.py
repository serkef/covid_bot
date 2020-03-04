""" Monitoring entrypoint """

import logging
import pprint
import sqlite3
from time import sleep

import pandas as pd

from gsheet_bot.utilities import (
    set_logging,
    tweet_status,
    get_gsheet_api,
    create_tables,
    slack_status,
    create_status,
)
from gsheet_bot.config import (
    GSHEET_POLLING_INTERVAL_SEC,
    GSHEET_SPREADSHEET_ID,
    GSHEET_SHEET_NAME,
    DB_PATH,
    DB_GET_LATEST_UPDATES,
    APP_LOGLEVEL,
    DB_GET_TOTAL_COUNTS,
)

GSHEET_API = get_gsheet_api()
DB_CONNECTION = sqlite3.connect(f"{DB_PATH}")


def fetch_daily_values():
    logger = logging.getLogger(f"{__name__}.fetch_daily_values")
    logger.debug("Fetching data")
    result = (
        GSHEET_API.values()
        .get(spreadsheetId=GSHEET_SPREADSHEET_ID, range=GSHEET_SHEET_NAME)
        .execute()
    )

    logger.debug("Processing fetched data")
    values = result.get("values", [])

    df = pd.DataFrame(values).iloc[3:, 1:61]
    df.columns = df.iloc[0]  # Set first line as headers
    df = (
        df.drop(df.index[0])  # Remove first row
        .set_index(df.columns[0])  # Set first column as index
        .unstack()  # transform to unpivoted
        .replace(r"^\s*$", "0", regex=True)  # replace empties
        .reset_index()  # Fix index
    )
    df.columns = ["rec_dt", "rec_territory", "rec_value"]
    df.rec_dt = pd.to_datetime(df.rec_dt, utc=True).dt.date
    df.rec_value = pd.to_numeric(
        df.rec_value.fillna("0").str.replace("+", "").str.replace(",", "")
    )
    df = df.sort_values(by=["rec_dt", "rec_territory"])

    logger.debug("Storing latest data")
    df.to_sql("latest_daily", DB_CONNECTION, if_exists="replace", index=False)
    df.to_sql("raw_data", DB_CONNECTION, if_exists="append", index=False)
    latest = pd.read_sql(DB_GET_LATEST_UPDATES, con=DB_CONNECTION)
    latest.to_sql("posts", DB_CONNECTION, if_exists="append", index=False)
    if len(latest) > 10:
        logger.warning("Too many changes. Won't post")
        logger.warning(pprint.pprint(latest.to_dict()))
        return

    logger.debug("Returning updates")
    for _, entry in latest.iterrows():
        total_count = DB_CONNECTION.execute(
            DB_GET_TOTAL_COUNTS.format(territory=entry.rec_territory)
        ).fetchone()
        yield total_count[0], entry.rec_dt, entry.rec_territory, entry.rec_value


def main():
    """ Main run function """

    logging.getLogger("googleapiclient.discovery").setLevel(APP_LOGLEVEL)
    logging.getLogger("google_auth_httplib2").setLevel(APP_LOGLEVEL)
    set_logging(APP_LOGLEVEL)
    logger = logging.getLogger(f"{__name__}.main")

    logger.info(f"Creating local db...")
    create_tables()

    logger.info(f"Starting...")
    while True:
        for total, day, territory, value in fetch_daily_values():
            status = create_status(total, day, territory, value)
            slack_status(status)
            tweet_status(status)

        logger.debug(f"Waiting {GSHEET_POLLING_INTERVAL_SEC} sec before polling gsheet")
        sleep(GSHEET_POLLING_INTERVAL_SEC)


if __name__ == "__main__":
    main()

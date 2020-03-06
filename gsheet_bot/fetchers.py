""" A module for all Fetcher classes """
import logging
import sqlite3

import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from gsheet_bot.config import (
    GSHEET_API_SERVICE_ACCOUNT_FILE,
    GSHEET_SHEET_NAME,
    GSHEET_SPREADSHEET_ID,
    DB_PATH,
    DB_GET_LATEST_UPDATES,
    DB_GET_TOTAL_COUNTS,
)


class GsheetFetcher:
    """ A generic fetcher for Google sheets. Knows how to auth and fetch. """

    def __init__(self, spreadsheet_id, spreadsheet_range, scopes=None):
        self.scopes = scopes or ["https://www.googleapis.com/auth/spreadsheets"]
        self.api = self.get_gsheet_api()
        self.spreadsheet_id = spreadsheet_id
        self.spreadsheet_range = spreadsheet_range
        self.db = sqlite3.connect(f"{DB_PATH}")

    def get_gsheet_api(self):
        """ Initializes Google API using service account """

        credentials = service_account.Credentials.from_service_account_file(
            filename=GSHEET_API_SERVICE_ACCOUNT_FILE, scopes=self.scopes
        )

        service = build("sheets", "v4", credentials=credentials)
        return service.spreadsheets()

    def data(self):
        """ Fetches data from gsheet """
        logger = logging.getLogger("GsheetFetcher.fetch")
        logger.debug("Fetching data")
        try:
            return (
                self.api.values()
                .get(spreadsheetId=self.spreadsheet_id, range=self.spreadsheet_range)
                .execute()
            )
        except HttpError as exc:
            logger.error("Cannot fetch values.", exc_info=True)
            return


class DailyData(GsheetFetcher):
    MAX_YIELD_SIZE = 10

    def __init__(self):
        super().__init__(
            spreadsheet_id=GSHEET_SPREADSHEET_ID, spreadsheet_range=GSHEET_SHEET_NAME
        )

    def fetch(self):
        """ Fetches and process gsheet data. Returns a well structured data frame """

        data = self.data()
        if data is None:
            return
        df = pd.DataFrame(data["values"]).iloc[3:, 1:61]
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

        return df

    def process(self):
        """ Processes data and stores to db """

        logger = logging.getLogger("DailyData.process")
        daily_data = self.fetch()
        if daily_data is None:
            logger.debug("Fetched empty dataset")
            return

        daily_data.to_sql("latest_daily", self.db, if_exists="replace", index=False)
        daily_data.to_sql("raw_data", self.db, if_exists="append", index=False)
        latest = pd.read_sql(DB_GET_LATEST_UPDATES, con=self.db)
        latest.to_sql("posts", self.db, if_exists="append", index=False)
        if len(latest) <= self.MAX_YIELD_SIZE:
            return latest
        logger.warning("Too many changes. Won't post")
        logger.warning(latest.to_dict())
        return

    def updates(self):
        """ Iterate over the latest fetched and processed """
        df = self.process()
        if df is None:
            return
        for _, entry in df.iterrows():
            total_count = self.db.execute(
                DB_GET_TOTAL_COUNTS.format(territory=entry.rec_territory)
            ).fetchone()
            yield total_count[0], entry.rec_dt, entry.rec_territory, entry.rec_value
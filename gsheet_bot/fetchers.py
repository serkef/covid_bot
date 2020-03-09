""" A module for all Fetcher classes """
import logging
import socket

import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from gsheet_bot.config import (
    GSHEET_API_SERVICE_ACCOUNT_FILE,
    GSHEET_SHEET_DAILY_NAME,
    GSHEET_SPREADSHEET_ID,
    DB_GET_LATEST_UPDATES,
    DB_GET_TOTAL_COUNTS,
    DbSession,
    DB_INSERT_RAW_DAILY_DATA,
    GSHEET_SHEET_LIVE_NAME,
    DB_INSERT_RAW_HOME_DATA,
)
from gsheet_bot.utilities import read_file


class GsheetFetcher:
    """ A generic fetcher for Google sheets. Knows how to auth and fetch. """

    def __init__(self, spreadsheet_id, spreadsheet_range, scopes=None):
        self.scopes = scopes or ["https://www.googleapis.com/auth/spreadsheets"]
        self.api = self.get_gsheet_api()
        self.spreadsheet_id = spreadsheet_id
        self.spreadsheet_range = spreadsheet_range
        self.db = DbSession().bind

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
        logger.debug("Fetching data...")
        try:
            return (
                self.api.values()
                .get(spreadsheetId=self.spreadsheet_id, range=self.spreadsheet_range)
                .execute()
            )
        except (HttpError, socket.timeout) as exc:
            logger.error("Cannot fetch values.", exc_info=True)
            return


class DailyData(GsheetFetcher):
    MAX_YIELD_SIZE = 10

    def __init__(self):
        super().__init__(
            spreadsheet_id=GSHEET_SPREADSHEET_ID,
            spreadsheet_range=GSHEET_SHEET_DAILY_NAME,
        )

    def fetch(self):
        """ Fetches and process gsheet data. Returns a well structured data frame """

        logger = logging.getLogger("DailyData.fetch")
        data = self.data()
        if data is None:
            logger.debug("Fetched no data")
            return
        logger.info("Processing fetched data...")
        df = pd.DataFrame(data["values"]).iloc[3:, 1:61]
        df.columns = df.iloc[0]  # Set first line as headers
        df = (
            df.drop(df.index[0])  # Remove first row
            .set_index(df.columns[0])  # Set first column as index
            .unstack()  # transform to unpivoted
            .replace(r"^\s*$", "0", regex=True)  # replace empties
            .reset_index()  # Fix index
        )
        df = df.drop(df[df[df.columns[1]].replace("", pd.NaT).isnull()].index)
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
        logger.info("Storing processed data...")

        daily_data.to_sql(
            "latest_daily_data", self.db, if_exists="replace", index=False
        )
        self.db.execute(
            read_file(DB_INSERT_RAW_DAILY_DATA),
            [
                (rec.rec_dt, rec.rec_territory, rec.rec_value)
                for _, rec in daily_data.iterrows()
            ],
        )
        latest = pd.read_sql(read_file(DB_GET_LATEST_UPDATES), con=self.db)
        latest.to_sql("post_daily_data", self.db, if_exists="append", index=False)
        if len(latest) <= self.MAX_YIELD_SIZE:
            return latest
        logger.warning("Too many changes. Won't post")
        logger.warning(latest.to_dict())

    def updates(self):
        """ Iterate over the latest fetched and processed """

        df = self.process()
        if df is None:
            return
        for _, entry in df.iterrows():
            total_count = self.db.execute(
                read_file(DB_GET_TOTAL_COUNTS).format(territory=entry.rec_territory)
            ).fetchone()
            yield int(total_count[0]), entry.rec_dt, entry.rec_territory, int(
                entry.rec_value
            )


class HomeData(GsheetFetcher):
    def __init__(self):
        super().__init__(
            spreadsheet_id=GSHEET_SPREADSHEET_ID,
            spreadsheet_range=GSHEET_SHEET_LIVE_NAME,
        )

    def fetch(self):
        """ Fetches and process gsheet data. Returns a well structured data frame """

        logger = logging.getLogger("HomeData.fetch")
        data = self.data()
        if data is None:
            logger.debug("Fetched no data")
            return
        logger.info("Processing fetched data...")
        df = pd.DataFrame(data["values"]).iloc[3:, [2, 4, 8, 13, 17, 21, 24]]
        df = df.replace(r"^\s*$", "0", regex=True).fillna(0).reset_index(drop=True)
        df = df.drop(df[df[df.columns[0]].replace("0", pd.NaT).isnull()].index)
        val_cols = ["cases", "deaths", "recovered", "severe", "tested", "active"]
        df.columns = ["rec_territory"] + val_cols
        for field in val_cols:
            df[field] = pd.to_numeric(df[field].fillna("0").str.replace(",", ""))

        return df

    def process(self):
        """ Processes data and stores to db """

        logger = logging.getLogger("HomeData.process")
        home_data = self.fetch()
        if home_data is None:
            logger.debug("Fetched empty dataset")
            return
        logger.info("Storing processed data...")

        home_data.to_sql("latest_home_data", self.db, if_exists="replace", index=False)
        self.db.execute(
            read_file(DB_INSERT_RAW_HOME_DATA),
            [
                (
                    rec.rec_territory,
                    rec.cases,
                    rec.deaths,
                    rec.recovered,
                    rec.severe,
                    rec.tested,
                    rec.active,
                )
                for _, rec in home_data.iterrows()
            ],
        )

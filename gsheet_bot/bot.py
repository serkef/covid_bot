""" Monitoring entrypoint """

import logging
from time import sleep

from gsheet_bot.config import GSHEET_POLLING_INTERVAL_SEC, APP_LOGLEVEL
from gsheet_bot.fetchers import DailyData
from gsheet_bot.utilities import (
    set_logging,
    tweet_status,
    create_tables,
    slack_status,
    create_status,
)


def main():
    """ Main run function """

    logging.getLogger("googleapiclient.discovery").setLevel(APP_LOGLEVEL)
    logging.getLogger("google_auth_httplib2").setLevel(APP_LOGLEVEL)
    set_logging(APP_LOGLEVEL)
    logger = logging.getLogger(f"{__name__}.main")

    logger.info(f"Creating local db...")
    create_tables()

    logger.info(f"Starting...")
    daily_fetcher = DailyData()
    while True:
        for total, day, territory, value in daily_fetcher.updates():
            status = create_status(total, day, territory, value)
            slack_status(status)
            tweet_status(status)

        logger.debug(f"Waiting {GSHEET_POLLING_INTERVAL_SEC} sec before polling gsheet")
        sleep(GSHEET_POLLING_INTERVAL_SEC)


if __name__ == "__main__":
    main()

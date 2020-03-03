# COVID-19

## Twitter bot
We post latest updates to twitter automatically 

* `bot.py` - Works in a continuous manner, check the proper fields in our spreadsheet and posts to twitter.
Users `tweepy` for posting to twitter. Using original google api lib for getting data from google spreadsheet.

In order to run:
* Make sure you have python > 3.6 installed `python3 --version`
* Create a virtual environment `python3 -m venv venv/`
* Install all dependencies `pip install -r requirements.txt`
* Add required environment variables
* Run the script `python3 bot.py`
* During the first run, you will have to authenticate the app for accessing the google account

### Env Variables
The following are required:
* `APP_LOGS` controls where logs are written
* `CONSUMER_KEY` Twitter consumer key
* `CONSUMER_KEY_SECRET` Twitter consumer key secret
* `ACCESS_TOKEN` Twitter access token
* `ACCESS_TOKEN_SECRET` Twitter access token secret
* `GSHEET_POLLING_INTERVAL_SEC` controls how often gsheet is polled. Defaults to 5
* `GSHEET_SPREADSHEET_ID` the Spreadsheet id
* `GSHEET_SHEET_NAME` the individual sheet name
* `ENV` controls whether tweets are actually posted or only logged. Defaults to `testing`. Turn to `production` to post.

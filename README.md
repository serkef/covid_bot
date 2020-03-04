# COVID-19 bot
A bot that monitors datasources for COVID-19


## How to run locally
*  Make sure you have python 3.8 `python --version`
* Create a virtual environment `python -m venv venv/`
* Activate venv `source venv/bin/activate`
* Install requirements `pip install -r requirements.txt`
* Add required [environment variables](#env-variables)
* Run the script `python3 bot.py`


### Environment Variables
The following are required:
* `APP_LOGS` controls where logs are written
* `APP_LOGLEVEL` controls the loglevel
* `CONSUMER_KEY` Twitter consumer key
* `CONSUMER_KEY_SECRET` Twitter consumer key secret
* `ACCESS_TOKEN` Twitter access token
* `ACCESS_TOKEN_SECRET` Twitter access token secret
* `GSHEET_API_SERVICE_ACCOUNT_FILE` The json file with google API service account credentials
* `GSHEET_POLLING_INTERVAL_SEC` controls how often gsheet is polled. Defaults to 60
* `GSHEET_SPREADSHEET_ID` the Spreadsheet id
* `GSHEET_SHEET_NAME` the individual sheet name
* `SLACK_WEBHOOK_URL` slack webhook URL
* `DB_PATH` the filepath of the sqlite3 db
* `POST_SLACK` when `true` posts to Slack are enabled 
* `POST_TWITTER` when `true` posts to Twitter are enabled 

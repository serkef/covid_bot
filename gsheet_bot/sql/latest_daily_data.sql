CREATE TABLE IF NOT EXISTS latest_daily_data (
    rec_dt DATE NOT NULL,
    rec_territory TEXT NOT NULL,
    rec_value NUMERIC,
    PRIMARY KEY (rec_dt, rec_territory)
)

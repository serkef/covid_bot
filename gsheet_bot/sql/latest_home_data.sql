CREATE TABLE IF NOT EXISTS latest_home_data (
    rec_territory TEXT NOT NULL,
    cases NUMERIC NOT NULL,
    deaths NUMERIC NOT NULL,
    recovered NUMERIC NOT NULL,
    severe NUMERIC NOT NULL,
    tested NUMERIC NOT NULL,
    active NUMERIC NOT NULL
)

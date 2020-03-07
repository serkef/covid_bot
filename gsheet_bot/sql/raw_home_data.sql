CREATE TABLE IF NOT EXISTS raw_home_data (
    id SERIAL,
    ts TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    rec_territory TEXT NOT NULL,
    cases NUMERIC NOT NULL,
    deaths NUMERIC NOT NULL,
    recovered NUMERIC NOT NULL,
    severe NUMERIC NOT NULL,
    tested NUMERIC NOT NULL,
    active NUMERIC NOT NULL,
    CONSTRAINT unique_home_entry UNIQUE(rec_territory, cases, deaths, recovered, severe, tested, active)
)

CREATE TABLE IF NOT EXISTS raw_daily_data (
    id SERIAL,
    ts TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    rec_dt DATE NOT NULL,
    rec_territory TEXT NOT NULL,
    rec_value NUMERIC,
    CONSTRAINT unique_entry UNIQUE(rec_dt, rec_territory, rec_value)
)

CREATE TABLE IF NOT EXISTS post_daily_data (
    id SERIAL,
    ts TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    rec_dt DATE NOT NULL,
    rec_territory TEXT NOT NULL,
    rec_value NUMERIC
)

INSERT INTO
    raw_daily_data (rec_dt, rec_territory, rec_value)
VALUES
    (%s, %s, %s)
ON CONFLICT ON CONSTRAINT unique_entry DO NOTHING

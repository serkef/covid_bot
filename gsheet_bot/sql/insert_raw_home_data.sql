INSERT INTO
    raw_home_data (rec_territory, cases, deaths, recovered, severe, tested, active)
VALUES
    (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT ON CONSTRAINT unique_home_entry DO NOTHING

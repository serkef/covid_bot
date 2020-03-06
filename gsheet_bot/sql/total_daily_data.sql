SELECT
    SUM(latest_daily_data.rec_value)
FROM
    latest_daily_data
WHERE
    latest_daily_data.rec_territory = '{territory}'

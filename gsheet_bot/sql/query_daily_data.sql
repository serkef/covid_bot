SELECT
    latest_daily_data.rec_dt,
    latest_daily_data.rec_territory,
    latest_daily_data.rec_value

FROM
    latest_daily_data
    LEFT JOIN (
        SELECT
            max(ts) AS ts,
            rec_dt,
            rec_territory,
            max(rec_value) AS rec_value
        FROM
            post_daily_data
        GROUP BY
            rec_dt,
            rec_territory
    ) AS latest_posts
              ON latest_daily_data.rec_territory = latest_posts.rec_territory
                      AND latest_daily_data.rec_dt = latest_posts.rec_dt

WHERE
      latest_daily_data.rec_value > 0
  AND (latest_posts.rec_value IS NULL OR latest_posts.rec_value < latest_daily_data.rec_value)
  AND (latest_posts.rec_value IS NULL OR latest_posts.ts > current_timestamp - interval '1 hour')
  AND current_date = latest_daily_data.rec_dt

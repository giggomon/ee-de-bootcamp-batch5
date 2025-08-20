{{ config(
    materialized='view'
) }}

WITH weekly_trips AS (
    SELECT
        d.year as year_num,
        d.month as month_num,
        d.week as week_num,
        d.week_start_date,
        d.week_end_date,
        COUNT(f.trip_id) AS total_trips
    FROM {{ ref('fact_taxi_trips') }} f
    LEFT JOIN {{ ref('dim_date') }} d 
        ON d.date_id = f.date_id
    GROUP BY d.year, d.month, d.week, d.week_start_date, d.week_end_date
),

weekly_growth AS (
     SELECT
        year_num,
        month_num,
        week_num,
        total_trips,
        week_start_date,
        week_end_date,
        LAG(total_trips) OVER (ORDER BY year_num, week_num) AS prev_week_trips
    FROM weekly_trips
)

SELECT
    year_num,
    month_num,
    week_num,
    week_start_date,
    week_end_date,
    total_trips,
    CASE 
        WHEN prev_week_trips IS NULL OR prev_week_trips = 0 THEN NULL
        ELSE ROUND(((total_trips - prev_week_trips) / prev_week_trips) * 100, 2)
    END AS wow_growth_rate
FROM weekly_growth
ORDER BY year_num, week_num
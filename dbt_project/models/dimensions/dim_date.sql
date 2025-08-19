{{ config(
    materialized='incremental',
    unique_key='date_id',
    tags=['fact_dim']
) }}

with dates as (
    SELECT DISTINCT
        cast(tpep_pickup_datetime AS date) AS date_val,
        created_timestamp AS updated_ts
    FROM {{ ref('taxi_trips_consistent') }}
    {% if is_incremental() %}
        where created_timestamp > (select max(updated_ts) from {{ this }})
    {% endif %}
)

SELECT
    cast(to_char(date_val, 'YYYYMMDD') as int) as date_id,
    date_val as date,

    --ISO compatible year/week
    YEAROFWEEKISO(date_val) as year,
    extract(month from date_val) as month,
    WEEKISO(date_val) as week,
    extract(day from date_val) as day,

    -- Week start = Monday of the ISO week
    dateadd(day, 1 - dayofweekiso(date_val), date_val) as week_start_date,

    -- Week end = Sunday (week_start + 6 days)
    dateadd(day, 7 - dayofweekiso(date_val), date_val) as week_end_date,

    updated_ts
FROM dates

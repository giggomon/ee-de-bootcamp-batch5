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
),

base AS (
    SELECT
        to_char(date_val, 'YYYYMMDD')::int AS date_id,
        date_val AS date,
        extract(year from date_val) AS year,
        extract(month from date_val) AS month,
        extract(week from date_val) AS week,
        extract(day from date_val) AS day,
        updated_ts
    FROM dates

    {% if is_incremental() %}
        WHERE updated_ts > (SELECT MAX(updated_ts) FROM {{ this }})
    {% endif %}
)

SELECT
    date_id,
    date,
    year,
    month,
    week,
    day,
    updated_ts
FROM base

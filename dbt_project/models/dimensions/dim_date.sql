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
    extract(year from date_val) as year,
    extract(month from date_val) as month,
    extract(week from date_val) as week,
    extract(day from date_val) as day,
    updated_ts
FROM dates

{{ config(
    materialized='incremental',
    unique_key='location_id',
    incremental_strategy='merge',
    tags=['fact_dim']
) }}

WITH locations AS (
    SELECT DISTINCT
        pickup_longitude,
        pickup_latitude,
        dropoff_longitude,
        dropoff_latitude,
        created_timestamp AS updated_ts
    FROM {{ ref('taxi_trips_consistent') }}

    {% if is_incremental() %}
        WHERE updated_ts > (SELECT max(updated_ts) FROM {{ this }})
    {% endif %}
)

SELECT
    md5(concat_ws('|',
        pickup_longitude,
        pickup_latitude,
        dropoff_longitude,
        dropoff_latitude)) AS location_id,
    pickup_longitude,
    pickup_latitude,
    dropoff_longitude,
    dropoff_latitude,
    updated_ts
FROM locations

-- Keep only the latest row for each unique coordinate set
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY pickup_longitude, pickup_latitude, dropoff_longitude, dropoff_latitude
    ORDER BY updated_ts DESC
) = 1

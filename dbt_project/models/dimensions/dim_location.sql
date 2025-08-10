{{ config(
    materialized='incremental',
    unique_key='location_id',
    incremental_strategy='merge',
    tags=['fact_dim']
) }}

WITH base AS (
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
        created_timestamp AS updated_ts
    FROM {{ ref('taxi_trips_consistent') }}

    {% if is_incremental() %}
        WHERE created_timestamp > (SELECT max(updated_ts) FROM {{ this }})
    {% endif %}
)

-- Keep only the latest row for each unique coordinate set

SELECT * 
FROM base
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY location_id
    ORDER BY updated_ts DESC
) = 1

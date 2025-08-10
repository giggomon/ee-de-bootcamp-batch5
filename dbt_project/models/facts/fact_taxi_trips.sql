{{ config(
    materialized='incremental',
    unique_key='trip_id',
    tags=['fact_dim']
) }}

WITH base AS (
    SELECT
        md5(concat_ws('|', vendor_id, tpep_pickup_datetime, pickup_longitude, pickup_latitude)) AS trip_id,  -- hashed surrogate key
        vendor_id,
        payment_type,
        CAST(tpep_pickup_datetime AS date) AS date_id,
        md5(concat_ws('|', pickup_longitude, pickup_latitude, dropoff_longitude, dropoff_latitude)) AS location_id,
        passenger_count,
        trip_distance,
        ratecode_id,
        store_and_fwd_flag,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        created_timestamp AS updated_ts
    FROM {{ ref('taxi_trips_consistent') }}

    {% if is_incremental() %}
    WHERE updated_ts > (SELECT max(updated_ts) FROM {{ this }})
    {% endif %}
),

calc AS (
    SELECT
        *,
        datediff(minute, tpep_pickup_datetime, tpep_dropoff_datetime) AS trip_duration_minutes,
        CASE WHEN datediff(minute, tpep_pickup_datetime, tpep_dropoff_datetime) > 0
            THEN round(trip_distance / (datediff(minute, tpep_pickup_datetime, tpep_dropoff_datetime) / 60.0), 2)
            ELSE null
        END AS trip_speed_mph
    FROM base
)

SELECT
    trip_id,
    vendor_id,
    payment_type AS payment_type_id,
    to_char(date_id, 'YYYYMMDD')::int AS date_id,
    location_id,
    passenger_count,
    trip_distance,
    ratecode_id,
    store_and_fwd_flag,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    trip_duration_minutes,
    trip_speed_mph,
    updated_ts
FROM calc
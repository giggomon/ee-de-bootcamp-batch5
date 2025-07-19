{{ config(
    materialized='table'
) }}

SELECT
    CAST("vendorid" AS INTEGER) AS vendor_id,
    CAST("tpep_pickup_datetime" AS TIMESTAMP) AS tpep_pickup_datetime,
    CAST("tpep_dropoff_datetime" AS TIMESTAMP) AS tpep_dropoff_datetime,
    CAST("passenger_count" AS INTEGER) AS passenger_count,
    CAST("trip_distance" AS NUMBER(10, 2)) AS trip_distance,
    CAST("pickup_longitude" AS NUMBER(18, 15)) AS pickup_longitude,
    CAST("pickup_latitude" AS NUMBER(18, 15)) AS pickup_latitude,
    CAST("ratecodeid" AS INTEGER) AS ratecode_id,
    CAST("store_and_fwd_flag" AS VARCHAR) AS store_and_fwd_flag,
    CAST("dropoff_longitude" AS NUMBER(18, 15)) AS dropoff_longitude,
    CAST("dropoff_latitude" AS NUMBER(18, 15)) AS dropoff_latitude,
    CAST("payment_type" AS INTEGER) AS payment_type,
    CAST("fare_amount" AS NUMBER(10, 2)) AS fare_amount,
    CAST("extra" AS NUMBER(10, 2)) AS extra,
    CAST("mta_tax" AS NUMBER(10, 2)) AS mta_tax,
    CAST("tip_amount" AS NUMBER(10, 2)) AS tip_amount,
    CAST("tolls_amount" AS NUMBER(10, 2)) AS tolls_amount,
    CAST("improvement_surcharge" AS NUMBER(10, 2)) AS improvement_surcharge,
    CAST("total_amount" AS NUMBER(10, 2)) AS total_amount,
    CAST("created_timestamp" AS TIMESTAMP) AS created_timestamp

FROM {{ source('raw', 'taxi_trips_raw') }}

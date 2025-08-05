{{ config(
    materialized='table',
    unique_key=['vendor_id', 'tpep_pickup_datetime', 'pickup_longitude', 'pickup_latitude'],
    partition_by='created_timestamp'
) }}

WITH base AS(
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
),

dq_fixed AS (
    
    -- Rule 4: vendor_id assign to 1 if null
    SELECT
        COALESCE(vendor_id, 1) AS vendor_id, 
    
        -- Rule 5: calculate tpep_pickup_datetime
        CASE
            -- pickup time is either missing or after drop-off time (incorrect)
            -- Try estimate the pickuptime if both dropoff time and trip_distance are available
            -- Assume average speed = 12 miles/hr. Calculate esitmated duration (hr) = (trip_distance /12)
            -- Convert to mins * 60, subtract it from tpep_dropoff_datetime to back-calculate the pickup time
            WHEN (tpep_pickup_datetime IS NULL OR tpep_pickup_datetime > tpep_dropoff_datetime)
                AND tpep_dropoff_datetime IS NOT NULL AND trip_distance IS NOT NULL THEN
                DATEADD(minute, -1 * (trip_distance / 12) * 60, tpep_dropoff_datetime)

            -- if pickup time is missing and the previous condition didn't meet (missing distance), use a default placeholder timestamp
            WHEN (tpep_pickup_datetime) IS NULL THEN
                TIMESTAMP '1900-01-01 00:00:00'

            -- if pickup time is valid, use it
            ELSE tpep_pickup_datetime
        END AS tpep_pickup_datetime,

        -- Rule 6: calculate tpep_dropoff_datetime
        CASE
            -- dropoff time is missing but pickup time & trip distance are valid, estimate dropoff time
            -- Assume average speed = 12 miles/hr. Calculate esitmated duration (hr) = (trip_distance / 12)
            -- Convert to mins *60, add that as estimate for drop off
            WHEN tpep_dropoff_datetime IS NULL 
                AND tpep_pickup_datetime IS NOT NULL 
                AND trip_distance IS NOT NULL THEN
                DATEADD(minute, (trip_distance / 12) * 60, tpep_pickup_datetime)

            -- dropoff time & pickup time & drip distance all missing
            WHEN (tpep_dropoff_datetime) IS NULL THEN
                TIMESTAMP '1900-01-01 00:00:00'
            
            -- if dropoff time is valid, use it    
            ELSE tpep_dropoff_datetime
        END AS tpep_dropoff_datetime,

        -- Rule 7: calculate trip_distance
        CASE    
            -- trip distance is zero & latitude & longtitude are valid && pickup and dropoff coordinates are different
            -- calculate trip distance using haversine formula
            WHEN trip_distance = 0 AND pickup_latitude != dropoff_latitude AND pickup_longitude != dropoff_longitude
                AND pickup_latitude != 0 AND dropoff_latitude != 0 AND pickup_longitude != 0 AND dropoff_longitude != 0 THEN
                12742 * ASIN(SQRT(
                    0.5 - COS(RADIANS(dropoff_latitude - pickup_latitude)) / 2
                    + COS(RADIANS(pickup_latitude)) * COS(RADIANS(dropoff_latitude))
                    * (1 - COS(RADIANS(dropoff_longitude - pickup_longitude))) / 2
                ))
            WHEN trip_distance IS NULL THEN -1
            ELSE trip_distance
        END AS trip_distance, 

        -- Rule 8: passenger count
        CASE 
            -- passenger count is zero, replace with average passenger count calculated from historical data
            WHEN passenger_count = 0 THEN (
                    SELECT ROUND(AVG(passenger_count), 1)
                    FROM {{ this }}
                    WHERE passenger_count > 0
                )
            ELSE passenger_count
        END AS passenger_count,
        
        pickup_longitude,
        pickup_latitude,
        dropoff_longitude,
        dropoff_latitude,
        ratecode_id,
        store_and_fwd_flag,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        created_timestamp
    FROM base
),

 -- Rule 1:
 -- If all mandatory fields (vendor_id, tpep_pickup_datetime, tpep_dropoff_datetime, pickup_longitude, pickup_latitude) contain nulls or zeros, 
 -- these records are filtered out as invalid

 -- Rule 2:
 -- If both pickup_longitude, pickup_latitude are null or zeros, these records are filtered out as invalid

 -- Rule 3: 
 -- If both dropoff_longitude, dropoff_latitude are null or zeros, these records are filtered out as invalid

filtered AS (
    SELECT * FROM dq_fixed
    WHERE NOT (
        (COALESCE(vendor_id, 0) = 0 AND tpep_pickup_datetime IS NULL AND tpep_dropoff_datetime IS NULL
        AND COALESCE(pickup_longitude, 0) = 0 AND COALESCE(pickup_latitude, 0) = 0)
        OR (COALESCE(pickup_longitude, 0) = 0 AND COALESCE(pickup_latitude, 0) = 0)
        OR (COALESCE(dropoff_longitude, 0) = 0 AND COALESCE(dropoff_latitude, 0) = 0)
    )
),

deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY vendor_id, tpep_pickup_datetime, pickup_longitude, pickup_latitude
            ORDER BY created_timestamp DESC
        ) AS row_num
    FROM filtered
)

SELECT * 
FROM deduped
WHERE row_num = 1

{% if is_incremental() %}
    AND created_timestamp > (SELECT MAX(created_timestamp) FROM {{ this }} ) 
{% endif %}
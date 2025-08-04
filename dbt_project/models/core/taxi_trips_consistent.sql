{{ config(
    materialized='incremental',
    unique_key=['vendor_id', 'tpep_pickup_datetime', 'pickup_longitude', 'pickup_latitude'],
    partition_by='created_timestamp'
) }}

SELECT
    *
FROM {{ ref('stg_taxi_trips_consistent') }}

{% if is_incremental() %}
WHERE created_timestamp > (SELECT MAX(created_timestamp) FROM {{ this }})
{% endif %}

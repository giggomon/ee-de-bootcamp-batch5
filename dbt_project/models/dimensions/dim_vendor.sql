{{ config(
    materialized='incremental',
    unique_key='vendor_id',
    tags=['fact_dim']
) }}

WITH base AS (
    SELECT DISTINCT
        vendor_id,
        CASE vendor_id
            WHEN 1 THEN 'Creative Mobile Technologies'
            WHEN 2 THEN 'VeriFone Inc.'
            ELSE 'Unknown'
        END AS vendor_name,
        created_timestamp AS updated_ts
    FROM {{ ref('taxi_trips_consistent') }}

    {% if is_incremental() %}
        WHERE updated_ts > (SELECT MAX(updated_ts) FROM {{ this }})
    {% endif %}
)

SELECT
    vendor_id,
    vendor_name,
    updated_ts
FROM base
{{ config(
    materialized='incremental',
    unique_key='payment_type_id',
    tags=['fact_dim']
) }}

WITH base AS(
    SELECT DISTINCT 
        payment_type AS payment_type_id,
        CASE payment_type
            WHEN 1 THEN 'Credit Card'
            WHEN 2 THEN 'Cash'
            WHEN 3 THEN 'No Charge'
            WHEN 4 THEN 'Dispute'
            WHEN 5 THEN 'Unknown'
            ELSE 'Other'
        END AS payment_type_name,
        created_timestamp AS updated_ts
    FROM {{ ref('taxi_trips_consistent') }}
    
    {% if is_incremental() %}
        WHERE updated_ts > (SELECT MAX(updated_ts) FROM {{ this }})
    {% endif %}
    )

SELECT
    payment_type_id,
    payment_type_name,
    updated_ts
FROM base

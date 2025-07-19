{% test pickup_before_dropoff(model, pickup_col, dropoff_col) %}
    -- Fails when drop‑off is *not* after pick‑up
    SELECT *
    FROM {{ model }}
    WHERE {{ dropoff_col }} <= {{ pickup_col }}
{% endtest %}
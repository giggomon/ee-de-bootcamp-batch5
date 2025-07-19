{% test non_zero(model, column_name) %}
    -- Fails when the value is literally 0 (or ‑0.00)
    SELECT *
    FROM {{ model }}
    WHERE {{ column_name }} = 0
{% endtest %}

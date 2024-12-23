{{
    config(
        materialized="incremental"

    )
}}

SELECT
    t.payment_date AS date,
    SUM(t.payment_amount + IFNULL(r.discount_value, 0)) AS total_revenue
FROM {{ ref('fact_transaction') }} AS t
LEFT JOIN {{ ref('dim_revenue_adjustment') }} AS r
    ON t.payment_key = r.revenue_adjustments_key
GROUP BY t.payment_date

ORDER BY t.payment_date

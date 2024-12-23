{{
    config(
        materialized="table"
    )
}}

SELECT
    t.customer_country,
    AVG(dateDiff('day', l.estimated_arrival, l.shipping_date)) AS avg_delivery_time
FROM {{ ref('fact_transaction') }} AS t
INNER JOIN {{ ref('dim_product_logistics') }} AS l
    ON t.transaction_key = l.product_logistics_key
    GROUP BY t.customer_country
ORDER BY avg_delivery_time

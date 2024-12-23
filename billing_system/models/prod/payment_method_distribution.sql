{{
    config(
        materialized="table"
    )
}}

SELECT
    p.payment_method_id,
    t.payment_id,
    p.payment_method_name,
    COUNT(t.payment_id) AS transaction_count
FROM {{ ref('fact_transaction') }} AS t
INNER JOIN {{ ref('dim_payment_details') }} AS p
    ON t.payment_key = p.payment_details_key
GROUP BY
    p.payment_method_id,
    t.payment_id,
    p.payment_method_name
ORDER BY transaction_count DESC

CREATE MATERIALIZED VIEW payment_group_one_minute
ENGINE = MergeTree()
ORDER BY tuple(session_start)
AS
SELECT
    session_start,
    COUNT(*) AS session_payments,
    SUM(PaymentAmount) AS total_payment_amount
FROM (
    SELECT
        if(PaymentDate - lagInFrame(PaymentDate) OVER (ORDER BY PaymentDate) > INTERVAL 1 MINUTE OR lagInFrame(PaymentDate) OVER (ORDER BY PaymentDate) IS NULL, PaymentDate, NULL) AS session_start,
        PaymentAmount
    FROM
        payments
    )
WHERE
    session_start IS NOT NULL
GROUP BY
    session_start
ORDER BY
    session_start;
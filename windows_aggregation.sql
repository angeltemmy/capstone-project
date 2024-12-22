CREATE MATERIALIZED VIEW Window_Aggregations
ENGINE = MergeTree()
ORDER BY (window_start)
AS
SELECT
    toStartOfInterval(PaymentDate, INTERVAL 1 HOUR) AS window_start,
    COUNT(*) AS total_payments,
    SUM(PaymentAmount) AS total_amount
FROM
    payments
GROUP BY
    window_start
ORDER BY
    window_start;
CREATE MATERIALIZED VIEW payments_WITHIN_one_week
ENGINE = MergeTree()
ORDER BY ("InvoiceID")
AS
SELECT
    invoices.InvoiceID,
    invoices.InvoiceDate,
    payments.PaymentDate,
    payments.PaymentAmount,
    invoices.TotalAmount,
    payments.PaymentMethods_MethodID
FROM
    invoices
INNER JOIN
    payments
ON
    invoices.InvoiceID = payments.Invoices_InvoiceID
WHERE
    payments.PaymentDate BETWEEN invoices.InvoiceDate AND invoices.InvoiceDate + INTERVAL 7 DAY
ORDER BY invoices.InvoiceID;
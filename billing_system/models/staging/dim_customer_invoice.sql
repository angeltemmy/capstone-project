{{
    config(
        materialized="view"
    )
}}

select
    {{ dbt_utils.generate_surrogate_key(['customer.CustomerID', 'invoices.InvoiceID']) }} as customer_invoice_key,
    customer.CustomerID as customer_id,
    customer.Name as customer_name,
    customer.country as country,
    invoices.InvoiceID as invoice_id,
    invoices.InvoiceDate as invoice_date,
    invoices.DueDate as due_date,
    invoices.TotalAmount as total_amount,
    payments.PaymentID as payment_status
from {{ ref('invoices') }} as invoices
left join {{ ref('customer') }} as customer
    on invoices.CustomerID = customer.CustomerID
left join {{ ref('payments') }} as payments
    on invoices.InvoiceID = payments.Invoices_InvoiceID
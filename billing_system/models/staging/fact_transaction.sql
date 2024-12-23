{{
    config(
        materialized="view"
    )
}}

select
    {{ dbt_utils.generate_surrogate_key(['invoice_details.InvoiceID', 'invoice_details.ProductID', 'customer.CustomerID']) }} as transaction_key,
    {{ dbt_utils.generate_surrogate_key(['customer.CustomerID']) }} as customer_key,
    {{ dbt_utils.generate_surrogate_key(['shipping_details.ShippingID']) }} as shipping_key,
    {{ dbt_utils.generate_surrogate_key(['payment_methods.MethodID' ]) }} as payment_method_key,
    {{ dbt_utils.generate_surrogate_key(['payment_status.StatusID']) }} as payment_status_key,
    {{ dbt_utils.generate_surrogate_key(['discounts.DiscountID']) }} as discount_key,
    {{ dbt_utils.generate_surrogate_key(['bank_details.BankDetailID']) }} as bank_detail_key,
    {{ dbt_utils.generate_surrogate_key(['payment_logs.LogID']) }} as payment_log_key,
    {{ dbt_utils.generate_surrogate_key(['invoice_details.TaxID']) }} as tax_key,
    {{ dbt_utils.generate_surrogate_key(['payments.PaymentID']) }} as payment_key,

    -- Invoice details
    invoice_details.InvoiceID as invoice_id,
    invoice_details.ProductID as product_id,
    invoice_details.Quantity as quantity,
    invoice_details.TaxID as tax_id,
    invoice_details.LineTotal as line_total,

    -- Customer details
    customer.CustomerID as customer_id,
    customer.Name as customer_name,
    customer.country as customer_country,

    -- Shipping details
    shipping_details.ShippingID as shipping_id,
    shipping_details.Address as shipping_address,
    shipping_details.ShippingDate as shipping_date,
    shipping_details.EstimatedArrival as estimated_arrival,

    -- Payment methods
    payment_methods.MethodID as payment_method_id,
    payment_methods.MethodName as payment_method_name,
    payment_methods.Description as payment_method_description,

    -- Payment status
    payment_status.StatusID as payment_status_id,
    payment_status.StatusName as payment_status_name,
    payment_status.Description as payment_status_description,

    -- Discounts
    discounts.DiscountID as discount_id,
    discounts.DiscountName as discount_name,
    discounts.DiscountValue as discount_value,

    -- Bank details
    bank_details.BankDetailID as bank_detail_id,
    bank_details.BankName as bank_name,
    bank_details.AccountNumber as account_number,
    bank_details.IBAN as iban,
    bank_details.BIC as bic,

    -- Payment logs
    payment_logs.LogID as payment_log_id,
    payment_logs.Timestamp as payment_log_timestamp,
    payment_logs.LogMessage as payment_log_message,

     -- Payment Details
    payments.PaymentID as payment_id,
    payments.PaymentDate as payment_date,
    payments.PaymentAmount as payment_amount,
    payments.Invoices_InvoiceID as payment_invoice_id

from (
    select
        InvoiceID,
        ProductID,
        Quantity,
        TaxID,
        LineTotal
    from {{ ref('invoice_details') }}
) as invoice_details
left join (
    select
        CustomerID,
        Name,
        country
    from {{ ref('customer') }}
) as customer
    on invoice_details.InvoiceID = customer.CustomerID
left join (
    select
        ShippingID,
        InvoiceID,
        Address,
        ShippingDate,
        EstimatedArrival
    from {{ ref('shipping_details') }}
) as shipping_details
    on invoice_details.InvoiceID = shipping_details.InvoiceID
left join (
    select
        MethodID,
        MethodName,
        Description
    from {{ ref('payment_methods') }}
) as payment_methods
    on invoice_details.InvoiceID = payment_methods.MethodID
left join (
    select
        StatusID,
        StatusName,
        Description
    from {{ ref('payment_status') }}
) as payment_status
    on invoice_details.InvoiceID = payment_status.StatusID
left join (
    select
        DiscountID,
        DiscountName,
        DiscountValue
    from {{ ref('discounts') }}
) as discounts
    on invoice_details.InvoiceID = discounts.DiscountID
left join (
    select
        BankDetailID,
        BankName,
        AccountNumber,
        IBAN,
        BIC
    from {{ ref('bank_details') }}
) as bank_details
    on invoice_details.InvoiceID = bank_details.BankDetailID
left join (
    select
        LogID,
        Timestamp,
        LogMessage
    from {{ ref('payment_logs') }}
) as payment_logs
    on invoice_details.InvoiceID = payment_logs.LogID
left join (
    select
        PaymentID,
        PaymentDate,
        PaymentAmount,
        Invoices_InvoiceID
    from {{ ref('payments') }}
) as payments
    on invoice_details.InvoiceID = payments.PaymentID




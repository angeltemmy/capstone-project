{{
    config(
        materialized="view"
    )
}}

select
    {{ dbt_utils.generate_surrogate_key(['product.ProductID', 'shipping_details.ShippingID']) }} as product_logistics_key,
    product.ProductID as product_id,
    product.ProductName as product_name,
    product.Price as price,
    shipping_details.ShippingID as shipping_id,
    shipping_details.ShippingDate as shipping_date,
    shipping_details.EstimatedArrival as estimated_arrival,
    invoice_details.InvoiceID as invoice_id,
    invoices.InvoiceDate as invoice_date

from {{ ref('invoice_details') }} as invoice_details
left join {{ ref('product') }} as product
    on invoice_details.ProductID = product.ProductID
left join {{ ref('shipping_details') }} as shipping_details
    on invoice_details.InvoiceID = shipping_details.InvoiceID
left join {{ ref('invoices') }} as invoices
    on invoice_details.InvoiceID = invoices.InvoiceID

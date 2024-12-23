{{
    config(
        materialized="view"
    )
}}

select
    {{ dbt_utils.generate_surrogate_key(['payments.PaymentID', 'payment_methods.MethodID ', 'payment_status.StatusID']) }} as payment_details_key,
    payments.PaymentID as payment_id,
    payment_methods.MethodID as payment_method_id,
    payment_methods.MethodName as payment_method_name,
    payments.PaymentDate as payment_date,
    payments.PaymentAmount as payment_amount,
    payment_status.StatusID as payment_status_id,
    payment_status.StatusName as payment_status_name
from {{ ref('payments') }} as payments
left join {{ ref('payment_methods') }} as methods
    on payments.PaymentMethods_MethodID = payment_methods.MethodID
left join {{ ref('payment_status') }} as status
    on payments.PaymentStatus_StatusID = payment_status.StatusID

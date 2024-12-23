
{{
    config(
        materialized="table"
    )
}}

select
    *
from {{ source('default', 'payment_logs') }}

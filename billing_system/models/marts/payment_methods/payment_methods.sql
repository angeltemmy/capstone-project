-- SQL logic for the payment_methods table
{{
    config(
        materialized="table"
    )
}}

select
    *
from {{ source('default', 'payment_methods') }}

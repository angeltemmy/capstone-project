-- SQL logic for the invoice_details table
{{
    config(
        materialized="table"
    )
}}

select
    *
from {{ source('default', 'invoice_details') }}

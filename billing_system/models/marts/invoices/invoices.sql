-- SQL logic for the invoices table
{{
    config(
        materialized="table"
    )
}}

select
  *
from {{ source('default', 'invoices') }}

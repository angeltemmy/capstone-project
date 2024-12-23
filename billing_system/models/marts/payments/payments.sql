-- SQL logic for the payments table
{{
    config(
        materialized="table"
    )
}}

select
   *
from {{ source('default', 'payments') }}

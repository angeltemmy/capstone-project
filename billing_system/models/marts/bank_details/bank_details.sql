-- SQL logic for the customer table
{{
    config(
        materialized="table"
    )
}}

select
    *
from {{ source('default', 'bank_details') }}
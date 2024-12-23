-- SQL logic for the discounts table
{{
    config(
        materialized="table"
    )
}}

select
   *
from {{ source('default', 'discounts') }}

-- SQL logic for the product table
{{
    config(
        materialized="table"
    )
}}

select
   *
from {{ source('default', 'product') }}

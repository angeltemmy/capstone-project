-- SQL logic for the taxrates table
{{
    config(
        materialized="table"
    )
}}

select
   *
from {{ source('default', 'taxrates') }}

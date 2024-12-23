{{
    config(
        materialized="table"
    )
}}

select
    *
from {{ source('default', 'shipping_details') }}

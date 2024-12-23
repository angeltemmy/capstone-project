{{
    config(
        materialized="view"
    )
}}

select
    {{ dbt_utils.generate_surrogate_key(['discounts.DiscountID', 'taxes.TaxID']) }} as revenue_adjustments_key,
    discounts.DiscountID as discount_id,
    discounts.DiscountName as discount_name,
    discounts.DiscountValue as discount_value,
    taxrates.TaxID as tax_id,
    taxrates.Rate as tax_rate,
    taxrates.TaxName as tax_name
from {{ ref('discounts') }} as discounts
left join {{ ref('taxrates') }} as taxes
    on discounts.DiscountID = taxrates.TaxID

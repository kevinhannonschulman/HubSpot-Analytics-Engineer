{{config(materialized='view')}}

with source as (
    select * from {{source('dbt_models', 'Amenities_Changelog')}}
)

, reformat as (
    select listing_id
    , date(change_at) as amenities_change_date --removed timestamp from amenities change date to simplify--
    , replace(amenities, '"', '') as amenities --removed quotes--
    from source
)

, final as (
    select listing_id
    , amenities_change_date
    , trim(amenities, ' []') as amenities --trimmed leading space and brackets--
    from reformat
    group by all
)

select * from final
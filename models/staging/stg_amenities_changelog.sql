with source as (
    select * from {{source('dbt_models', 'Amenities_Changelog')}}
)

, reformat as (
    select listing_id
    , date(change_at) as amenities_change_date --removed timestamp from amenities change date to simplify--
    , replace(amenities, '"', '') as list_amenities_changes --removed quotes from amenities json--
    from source
)

, final as (
    select listing_id
    , amenities_change_date
    , trim(list_amenities_changes, '[]') as list_amenities_changes --trimmed brackets from amenities json--
    from reformat
)

select * from final
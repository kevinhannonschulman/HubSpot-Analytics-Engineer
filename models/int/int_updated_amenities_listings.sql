--verifying that amenities in stg_listings are actually up-to-date--
--dbt_expectations.expect_column_pair_values_to_be_equal test within _int_models.yml passed so stg_listings is up-to-date--

{{config(materialized='view')}}

with stg_amenities_changelog as (
    select * from {{ref('stg_amenities_changelog')}}
)

, stg_listings as (
    select * from {{ref('stg_listings')}}
)

--using the row_number window function to sort amenity changes by listing_id in chronological order and only selecting the most recent--

, amenities_update as (
    select *
    , row_number() over (partition by listing_id order by amenities_change_date desc) as row_number
    from stg_amenities_changelog
)

, current_amenities_final as (
    select listing_id
    , amenities_change_date
    , amenities
    from amenities_update 
    where row_number = 1)

, final as (
    select l.listing_id, l.neighborhood, c.amenities_change_date, c.amenities as current_amenities, l.amenities as stg_listings_amenities
    from stg_listings l
    inner join current_amenities_final c on l.listing_id = c.listing_id
    group by all
)

select * from final
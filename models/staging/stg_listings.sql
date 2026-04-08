{{ config(materialized='view') }}

with source as (
    select * from {{source('dbt_models', 'Listings')}}
)

--narrowing data down to the columns that will answer questions 1-3 and excluding extraneous information--

, reformat as (
    select id as listing_id
    , host_location
    , neighborhood
    , replace((amenities), '"', '') as amenities --removed quotes--
    from source
)

, final as (
    select listing_id
    , host_location
    , neighborhood
    , trim((amenities), '[]') as amenities --removed brackets. using replace with both brackets and quotes didn't remove both so needed to use two CTEs--
    from reformat
)

select * from final
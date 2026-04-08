with source as (
    select * from {{source('dbt_models', 'Listings')}}
)

, reformat as (
    select id as listing_id
    , name as property_name
    , host_id
    , date(host_since) as host_join_date
    , host_location
    , replace(host_verifications, '"', '') as host_verifications --removed quotes from json--
    , neighborhood
    , property_type
    , room_type
    , accommodates as max_people
    , bathrooms_text as num_baths
    , bedrooms
    , beds
    , replace((amenities), '"', '') as amenities --removed quotes from json--
    from source
)

, final as (
    select listing_id
    , property_name
    , host_id
    , host_join_date
    , host_location
    , trim(host_verifications, '[]') as host_verifications --removed brackets from json--
    , neighborhood
    , property_type
    , room_type
    , max_people
    , num_baths
    , bedrooms
    , beds
    , trim((amenities), '[]') as amenities --removed brackets from json--
    from reformat
)

select * from final
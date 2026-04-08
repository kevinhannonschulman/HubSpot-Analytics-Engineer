{{ config(materialized='view') }}

with source as (
    select * from {{ source('dbt_models', 'Calendar')}}
)

, final as (
    select listing_id
    , date as reservation_date
    , available as room_availability
    , reservation_id
    , price
    , minimum_nights
    , maximum_nights
    from source
)

select * from final
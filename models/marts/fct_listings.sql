{{config(materialized='table')}}
 
 --materializing the mart model as a table for improved efficiency for downstream exposures e.g. dashboards--

 with stg_calendar as (
    select * from {{ref('stg_calendar')}}
)

, int_updated_amenities_listings as (
    select * from {{ref('int_updated_amenities_listings')}}
)

, final as (
    select u.listing_id
    , c.reservation_date
    , u.neighborhood
    , u.current_amenities as amenities
    , u.amenities_change_date as last_date_amenity_change
    , c.room_availability
    , c.reservation_id
    , c.price
    , c.minimum_nights
    , c.maximum_nights
    from stg_calendar c
    left join int_updated_amenities_listings u on c.listing_id = u.listing_id
    group by all
)

select * from final
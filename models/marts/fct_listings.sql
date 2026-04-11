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

, eligible_rentals as (
    select listing_id
    , reservation_date
    , room_availability
    , maximum_nights
    , amenities
    from final
    where amenities like '%Lockbox%' and amenities like '%First aid kit%' and room_availability is true
)
--ranking the date column partitioned by listing_id which will allow calculation of consecutive days--
, datecount as (
    select listing_id
    , reservation_date
    , row_number() over (partition by listing_id order by reservation_date) as rnk
    from eligible_rentals
)
--subtracting rank from reservation_date will create groups a.k.a islands of consecutive days--
, dategroups as (
    select listing_id
    , reservation_date
    , reservation_date - (interval 1 day) * rnk as date_group
    from datecount
)
--finding start/end dates and calculating number of consecutive days within each interval group--
, consecutive as (
    select listing_id
    , min(reservation_date) as interval_start
    , max(reservation_date) as interval_end
    , 1 + date_diff(max(reservation_date), min(reservation_date), day) as max_consecutive
    from dategroups
    group by listing_id, date_group
    order by max_consecutive desc
)
--joining ctes to include maximum days allowed by each rental property--
, longest_possible_stay as (
    select e.listing_id
    , c.interval_start
    , c.interval_end
    , e.maximum_nights as maximum_days_allowed
    , c.max_consecutive as maximum_days_available
    from eligible_rentals e
    inner join consecutive c on e.listing_id = c.listing_id
    group by all
    order by c.max_consecutive desc
)

select * from longest_possible_stay
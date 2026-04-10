1. Write a query to find the total revenue and percentage of revenue by month segmented
by whether or not air conditioning exists on the listing.

month_extract as (
    select date_trunc(reservation_date, month) as reservation_month
    , amenities
    , price
    , room_availability
    from final
    where room_availability is false
)

, ac_revenue as (select distinct reservation_month as reservation_month
    , sum(price) over (partition by reservation_month) as monthly_revenue_with_ac
    from month_extract
    where amenities like '%Air conditioning%')

, no_ac_revenue as (select distinct reservation_month
    , sum(price) over (partition by reservation_month) as monthly_revenue_without_ac
    from month_extract
    where amenities not like '%Air conditioning%')

, revenue_breakdown as (
    select a.reservation_month
    , a.monthly_revenue_with_ac
    , n.monthly_revenue_without_ac
    , (a.monthly_revenue_with_ac + n.monthly_revenue_without_ac) as total_monthly_revenue
    , (a.monthly_revenue_with_ac / (a.monthly_revenue_with_ac + n.monthly_revenue_without_ac))*100 as revenue_percent_with_ac
    , (n.monthly_revenue_without_ac / (a.monthly_revenue_with_ac + n.monthly_revenue_without_ac))*100 as revenue_percent_without_ac
    from ac_revenue a
    inner join no_ac_revenue n on a.reservation_month = n.reservation_month

2. Write a query to find the average price increase for each neighborhood from July 12th
2021 to July 11th 2022 .

start_window as (
    select neighborhood
    , avg(price) over (partition by neighborhood) as avg_neighborhood_price_start
    , reservation_date
    from final
    where neighborhood is not null and reservation_date = '2021-07-12'
)

, end_window as (
    select neighborhood
    , avg(price) over (partition by neighborhood) as avg_neighborhood_price_end
    , reservation_date
    from final
    where neighborhood is not null and reservation_date = '2022-07-11')

, avg_price_increase as (
    select distinct(s.neighborhood)
    , s.avg_neighborhood_price_start
    , e.avg_neighborhood_price_end
    , (e.avg_neighborhood_price_end - s.avg_neighborhood_price_start) as avg_increase
    from start_window s
    join end_window e on s.neighborhood = e.neighborhood
)

select * from avg_price_increase


v2 (returns null values)


, avg_neighborhood_prices as (
    select neighborhood
    , avg(price) over (partition by neighborhood order by reservation_date) as avg_price
    , reservation_date
    from final
    where neighborhood is not null
)


, calculate as (
    select distinct (neighborhood)
    , avg_price
    , reservation_date
    , lag(avg_price, 364) over (order by reservation_date) as previous_avg_price
    from avg_neighborhood_prices
)

select * from calculate

3. Write a query to determine the longest possible stay duration for rental listings that
include both a lockbox and first aid kit in their amenities, considering both listing
availability windows and maximum stay limits set by property owners.

--only selecting rentals that match the amenity criteria, room_availability is true will create gaps for gaps and islands--

, eligible_rentals as (
    select listing_id
    , reservation_date
    , room_availability
    , maximum_nights
    , amenities
    from final
    where amenities like '%Lockbox%' and amenities like '%First aid kit%' and room_availability is true
)

, datecount as (
    select listing_id
    , reservation_date
    , dense_rank() over (partition by listing_id order by reservation_date) as rnk
    from eligible_rentals
)

, dategroups as (
    select listing_id
    , reservation_date
    , reservation_date - (interval 1 day) * rnk as date_group
    from datecount
)

, consecutive as (
    select listing_id
    , min(reservation_date) as interval_start
    , max(reservation_date) as interval_end
    , 1 + date_diff(max(reservation_date), min(reservation_date), day) as max_consecutive
    from dategroups
    group by listing_id, date_group
    order by max_consecutive desc
)

, longest_possible_stay as (
    select e.listing_id
    , c.interval_start
    , c.interval_end
    , e.maximum_nights as maximum_days_allowed
    , c.max_consecutive as maximum_days_available
    from eligible_rentals e
    inner join consecutive c on e.listing_id = c.listing_id
    order by c.max_consecutive desc
)

select * from longest_possible_stay
1. Write a query to find the total revenue and percentage of revenue by month segmented
by whether or not air conditioning exists on the listing.

'''sql
--revenue = when room is booked therefore room_availability is false--
month_extract as (
    select date_trunc(reservation_date, month) as reservation_month
    , amenities
    , price
    , room_availability
    from final
    where room_availability is false
)
--using window functions to calculate revenue for A/C and non-A/C units partitioned by month--
, ac_revenue as (select reservation_month
    , sum(price) over (partition by reservation_month) as monthly_revenue_with_ac
    from month_extract
    where amenities like '%Air conditioning%')

, no_ac_revenue as (select reservation_month
    , sum(price) over (partition by reservation_month) as monthly_revenue_without_ac
    from month_extract
    where amenities not like '%Air conditioning%')
--joining ctes to find total revenue/percentage of revenue from each type of unit--
, revenue_breakdown as (
    select a.reservation_month
    , a.monthly_revenue_with_ac
    , n.monthly_revenue_without_ac
    , (a.monthly_revenue_with_ac + n.monthly_revenue_without_ac) as total_monthly_revenue
    , (a.monthly_revenue_with_ac / (a.monthly_revenue_with_ac + n.monthly_revenue_without_ac))*100 as revenue_percent_with_ac
    , (n.monthly_revenue_without_ac / (a.monthly_revenue_with_ac + n.monthly_revenue_without_ac))*100 as revenue_percent_without_ac
    from ac_revenue a
    inner join no_ac_revenue n on a.reservation_month = n.reservation_month
    group by all
)

select * from revenue_breakdown
'''
2. Write a query to find the average price increase for each neighborhood from July 12th
2021 to July 11th 2022.
'''sql
--using window function to calculate average price partitioned by neighborhood on start date and end date--
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
--joining ctes to find average price increase in each neighborhood over given time period--
, avg_price_increase as (
    select s.neighborhood
    , s.avg_neighborhood_price_start
    , e.avg_neighborhood_price_end
    , (e.avg_neighborhood_price_end - s.avg_neighborhood_price_start) as avg_increase
    from start_window s
    inner join end_window e on s.neighborhood = e.neighborhood
    group by all
)

select * from avg_price_increase
'''
3. Write a query to determine the longest possible stay duration for rental listings that
include both a lockbox and first aid kit in their amenities, considering both listing
availability windows and maximum stay limits set by property owners.
'''sql
--selecting rentals that match amenity criteria, room_availability is true will create gaps for gaps and islands problem--

, eligible_rentals as (
    select listing_id
    , reservation_date
    , room_availability
    , maximum_nights
    , amenities
    from final
    where amenities like '%Lockbox%' and amenities like '%First aid kit%' and room_availability is true
)
--row_number() will always be consecutive but reservation_date won't because there will be gaps when room_availability is false--
, datecount as (
    select listing_id
    , reservation_date
    , row_number() over (partition by listing_id order by reservation_date) as rnk
    from eligible_rentals
)
--subtracting rank from reservation_date will create groups a.k.a islands of consecutive days--
--island_start_date will remain the same when reservation_dates are consecutive because - (interval 1 day) * rnk will always return to same date--
, dategroups as (
    select listing_id
    , reservation_date
    , reservation_date - (interval 1 day) * rnk as island_start_date
    from datecount
)
--island_start_date remains the same during consecutive streaks so can group by islands to find start/end date and number of consecutive days--
, consecutive as (
    select listing_id
    , min(reservation_date) as interval_start
    , max(reservation_date) as interval_end
    , 1 + date_diff(max(reservation_date), min(reservation_date), day) as max_consecutive --add 1 to include start/end date--
    from dategroups
    group by listing_id, island_start_date
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
'''
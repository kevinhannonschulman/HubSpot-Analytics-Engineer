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

3. Write a query to determine the longest possible stay duration for rental listings that
include both a lockbox and first aid kit in their amenities, considering both listing
availability windows and maximum stay limits set by property owners.
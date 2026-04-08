with source as (
    select * from {{source('dbt_models', 'Listings')}}
)

select * from source
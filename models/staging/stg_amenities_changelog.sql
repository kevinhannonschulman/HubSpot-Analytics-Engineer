with source as (
    select * from {{source('dbt_models', 'Amenities_Changelog')}}
)

select * from source
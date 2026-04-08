with source as (
    select * from {{ source('dbt_models', 'Calendar')}}
)

select * from source
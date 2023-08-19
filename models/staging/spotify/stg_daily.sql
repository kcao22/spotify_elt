with daily as (
    select  
        *
    from {{ source('spotify', 'daily') }}
)

select * from daily
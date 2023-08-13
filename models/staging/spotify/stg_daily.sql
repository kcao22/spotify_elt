with daily as (
    select  
        *
    from {{ ref('daily_snapshots') }}
)

select * from daily
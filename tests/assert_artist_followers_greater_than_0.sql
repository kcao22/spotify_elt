with daily as (
    select *
    from {{ ref('stg_daily') }}
)

select *
from daily
where artist_followers < 0
with daily as (
    select *
    from {{ ref('stg_daily') }}
)

select 
    track_popularity
from daily
where track_popularity not between 0 and 100
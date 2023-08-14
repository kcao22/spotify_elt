with daily as (
    select *
    from {{ ref('stg_daily') }}
)

select 
    artist_popularity
from daily
where artist_popularity not between 0 and 100
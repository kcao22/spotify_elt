with daily as (
    select *
    from {{ ref('stg_daily') }}
)

select 
    album_popularity
from daily
where album_popularity not between 0 and 100
with daily as (
    select *
    from {{ ref('stg_daily') }}
)

select *
from daily
where album_total_tracks < 0
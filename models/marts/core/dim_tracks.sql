{{ config(
    materialized='incremental'
    , incremental_strategy='merge'
    , unique_key=['track_id', 'valid_from']
)}}

with deduped_tracks_daily as (
    select
        track_id
        , track_name
        , track_url
        , track_length_ms
        , track_popularity
        , date_appended
    from {{ ref('stg_daily') }}
    {% if is_incremental() %}
        where date_appended >= (select dateadd(day, -1, max(valid_from)) from {{ this }})
    {% endif %}
    group by    
        track_id
        , track_name
        , track_url
        , track_length_ms
        , track_popularity
        , date_appended
), tracks as (
    select
        track_id
        , track_name
        , track_url
        , round(
            cast(track_length_ms as float) / 60000,
            2
        ) as track_length_min
        , track_popularity
        , date_appended as valid_from
        , lead(date_appended, 1) over(partition by track_id order by date_appended) as valid_to
    from deduped_tracks_daily
)
select 
    *
from tracks
{{ config(
    materialized='incremental'
    , incremental_strategy='merge'
    , unique_key=['album_id', 'valid_from']
)}}

with deduped_albums_daily as (
    select 
        album_id
        , album_name
        , album_url
        , album_popularity
        , album_total_tracks
        , album_release_date
        , date_appended
    from {{ ref('stg_daily') }}
    {% if is_incremental() %}
        where date_appended >= (select dateadd(day, -1, max(valid_from)) from {{ this }})
    {% endif %}
    group by 
        album_id
        , album_name
        , album_url
        , album_popularity
        , album_total_tracks
        , album_release_date
        , date_appended
), albums as (
    select 
        album_id
        , album_name
        , album_url
        , album_popularity
        , album_total_tracks
        , album_release_date
        , date_appended as valid_from
        , lead(date_appended, 1) over(partition by album_id order by date_appended) as valid_to
    from deduped_albums_daily
)
select 
    album_id
        , album_name
        , album_url
        , album_popularity
        , album_total_tracks
        , album_release_date
        , valid_from
        , valid_to
from albums
group by 
        album_id
        , album_name
        , album_url
        , album_popularity
        , album_total_tracks
        , album_release_date
        , valid_from
        , valid_to
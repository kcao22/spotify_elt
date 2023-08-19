{{ config(
    materialized='incremental'
    , incremental_strategy='merge'
    , unique_key=['artist_id', 'valid_from']
)}}

with deduped_artists_daily as (
    select 
        artist_id
        , artist_name
        , artist_url
        , artist_followers
        , artist_popularity
        , date_appended
    from {{ ref('stg_daily') }}
    {% if is_incremental () %}
        where date_appended >= (select dateadd(day, -1, max(valid_from)) from {{ this }})
    {% endif %}
    group by 
        artist_id
        , artist_name
        , artist_url
        , artist_followers
        , artist_popularity
        , date_appended
), artists as (
    select 
        artist_id
        , artist_name
        , artist_url
        , artist_followers
        , artist_popularity
        , date_appended as valid_from
        , lead(date_appended, 1) over(partition by artist_id order by date_appended) as valid_to
    from deduped_artists_daily
)
select 
    *
from artists
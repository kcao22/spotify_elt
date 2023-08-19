{{ config(
    materialized='incremental'
    , incremental_strategy='merge'
    , unique_key=['track_id', 'date_appended']
)}}

with listening_history as (
    select
        time_track_key
        , track_id
        , artist_id
        , album_id
        , played_at
        , date_appended
    
    from {{ ref("stg_daily") }}
    {% if is_incremental() %}
        where date_appended > (select max(date_appended) from {{this}})
    {% endif %}
)

select * from listening_history
with listening_history as (
    select
        time_track_key
        , track_id
        , artist_id
        , album_id
        , played_at
        , TO_DATE(date_appended) AS date_appended
    
    from {{ ref("stg_daily") }}
)

select * from listening_history
select  
    TIME_TRACK_KEY as time_track_key
    , TRACK_ID as track_id
    , TRACK_NAME as track_name
    , TRACK_URL as track_url
    , TRACK_LENGTH_MS as track_length_ms
    , TRACK_POPULARITY as track_popularity
    , ARTIST_ID as artist_id
    , ARTIST_NAME as artist_name 
    , ARTIST_URL as artist_url 
    , ARTIST_FOLLOWERS as artist_followers
    , ARTIST_POPULARITY as artist_popularity
    , ALBUM_ID as album_id
    , ALBUM_NAME as album_name
    , ALBUM_URL as album_url
    , ALBUM_POPULARITY as album_popularity
    , ALBUM_TOTAL_TRACKS as album_total_tracks
    , ALBUM_RELEASE_DATE as album_release_date
    , PLAYED_AT as played_at
    , DATE_APPENDED as date_appended
    
from {{ source('spotify', 'daily') }}
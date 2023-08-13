{% snapshot all_snapshots %}

{{
    config(
        target_database='analytics',
        target_schema='snapshots',
        unique_key='artist_id',

        strategy='timestamp',
        updated_at='date_appended'
    )
}}

select
  time_track_key
  , track_id
  , track_name
  , track_url
  , track_length_ms
  , track_popularity
  , artist_id
  , artist_name
  , artist_url
  , artist_followers
  , artist_popularity
  , album_id
  , album_name
  , album_url
  , album_popularity
  , album_total_tracks
  , album_release_date
  , played_at
  , CONVERT_TIMEZONE('UTC', date_appended) as date_appended
from {{ source('spotify', 'daily') }}

{% endsnapshot %}


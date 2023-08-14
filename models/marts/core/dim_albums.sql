with albums as (
    select
        album_id
        , album_name
        , album_url
        , album_popularity
        , album_total_tracks
        , album_release_date
        , dbt_valid_from as valid_from
        , dbt_valid_to as valid_to

    from {{ ref('stg_daily') }}
)

select * from albums
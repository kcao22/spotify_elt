with artists as (
    select
        artist_id
        , artist_name
        , artist_url
        , artist_followers
        , artist_popularity
        , dbt_valid_from as valid_from
        , dbt_valid_to as valid_to

    from {{ ref('stg_daily') }}
)

select * from artists
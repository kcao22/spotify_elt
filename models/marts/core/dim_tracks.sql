with tracks as (
    select
        track_id
        , track_name
        , track_url
        , round(
            cast(track_length_ms as float) / 60000,
            2
        ) track_length_min
        , track_popularity
        , dbt_valid_from as valid_from
        , dbt_valid_to as valid_to

    from {{ ref("stg_daily") }}
)

select * from tracks
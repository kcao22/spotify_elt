with tracks as (
    select
        track_id
        , track_name
        , track_url
        , track_length_ms
        , track_popularity
        , dbt_valid_from as valid_from
        , dbt_valid_to as valid_to

    from {{ ref("stg_daily") }}
)

select * from tracks
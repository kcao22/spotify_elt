{% snapshot artists_snapshot %}

{{
    config(
        target_database='analytics',
        target_schema='snapshots'
        unique_key='artist_id',
        strategy='date_appended'
    )
}}

select
  *
from {{ source('spotify', 'daily') }}

{% endsnapshot %}
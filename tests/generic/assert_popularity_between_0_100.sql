{% test popularity_between_0_100(model, column_name) %}

with validation as (
    select 
        {{ column_name }} as popularity_field
    from {{ model }}
),

validation_errors as (
    select 
        popularity_field
    from validation
    where {{ column_name }} not between 0 and 100
)
select *
from validation_errors

{% endtest %}
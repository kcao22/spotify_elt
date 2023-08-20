{% test popularity_between_0_100(model, column_name) %}

with validation as (
    select 
        {{ column_name }}
    from {{ model }}
),

validation_errors as (
    select 
        {{ column_name }}
    from validation
    where {{ column_name }} not between 0 and 100
)
select *
from validation_errors

{% endtest %}
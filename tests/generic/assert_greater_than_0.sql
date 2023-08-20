{% test greater_than_0(model, column_name) %}

with validation as (
    select 
        {{ column_name }}
    from {{ model }}
),

validation_errors as (
    select 
        {{ column_name }}
    from validation
    where {{ column_name }} < 0
)
select *
from validation_errors

{% endtest %}
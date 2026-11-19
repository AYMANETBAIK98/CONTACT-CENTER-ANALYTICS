{# Generic test: the SQL expression must be true on every row (like dbt_utils.expression_is_true, without the package). #}
{% test dbt_utils_expression(model, column_name, expression) %}
select *
from {{ model }}
where not ({{ expression }})
{% endtest %}

{% macro no_nulls_in_column(model) %}
    select * from {{ model }}
    where 
    {% for col in adapter.get_columns_in_relation(model) %}
        {{ col.column }} is null or
    {% endfor %}
    FALSE
{% endmacro %}
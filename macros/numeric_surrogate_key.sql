{% macro numeric_surrogate_key(columns) %}
    cast(
        farm_fingerprint(
            concat(
                {% for column in columns %}
                    coalesce(cast({{ column }} as string), ''){% if not loop.last %}, {% endif %}
                {% endfor %}
            )
        ) as int64
    )
{% endmacro %}

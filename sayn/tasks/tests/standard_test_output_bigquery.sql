{% set dst_schema = schema+'.' if schema else '' %}

-- Test {{ type }} on Column {{ name }} --
SELECT CAST(l.{{ name }} AS STRING) AS val
, COUNT(*) AS cnt
FROM {{ dst_schema }}{{ table }} AS l
{%- if type == 'not_null' %}
WHERE l.{{ name }} IS NULL
{%- elif type == 'allowed_values' %}
WHERE l.{{ name }} NOT IN ( {{ allowed_values }} )
{%- endif %}
GROUP BY l.{{ name }}
HAVING COUNT(*) > {%- if type == 'unique' %} 1 {%- else %} 0 {%- endif %}
;

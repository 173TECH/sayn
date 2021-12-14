{% set dst_schema = schema+'.' if schema else '' %}

SELECT  *
FROM (SELECT CAST(l.{{ name }} AS VARCHAR) AS val
     , COUNT(*) AS cnt
     , '{{ type }}' AS type
     , '{{ name }}' AS col
  FROM {{ dst_schema }}{{ table }} AS l
{%- if type == 'not_null' %}
 WHERE l.{{ name }} IS NULL
{%- elif type == 'values' %}
 WHERE l.{{ name }} NOT IN ( {{ values }} )
{%- endif %}
 GROUP BY l.{{ name }}
HAVING COUNT(*) > {%- if type == 'unique' %} 1 {%- else %} 0 {%- endif %}
ORDER BY cnt DESC
LIMIT 5) AS t

UNION ALL

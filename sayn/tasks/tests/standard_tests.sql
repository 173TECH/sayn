SELECT CAST(l.{{ name }} AS VARCHAR) AS col
     , COUNT(*) AS cnt
     , '{{ type }}' AS type
  FROM {{ table }} AS l
{%- if type == 'not_null' %}
 WHERE l.{{ name }} IS NULL
{%- elif type == 'values' %}
WHERE l.{{ name }} NOT IN ( {{ values }} )
{%- endif %}
 GROUP BY l.{{ name }}
HAVING COUNT(*) > {%- if type == 'unique' %} 1 {%- else %} 0 {%- endif %}

UNION ALL

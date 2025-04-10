{%- if temporary %}
CREATE TEMP TABLE {{ full_name }}
{%- elif not replace %}
CREATE TABLE IF NOT EXISTS {{ full_name }}
{%- elif replace and can_replace_table %}
  {%- if table_exists %}
CREATE OR REPLACE TABLE {{ full_name }}
  {%- elif view_exists %}
DROP VIEW IF EXISTS {{ full_name }}{{ ' CASCADE' if needs_cascade else ''}};
CREATE TABLE {{ full_name }}
  {%- else %}
CREATE OR REPLACE TABLE {{ full_name }}
  {%- endif %}
{%- elif replace and not can_replace_table %}
  {% if table_exists %}
DROP TABLE IF EXISTS {{ full_name }}{{ ' CASCADE' if needs_cascade else ''}};
  {% elif view_exists %}
DROP VIEW IF EXISTS {{ full_name }}{{ ' CASCADE' if needs_cascade else ''}};
  {% endif %}

CREATE TABLE {{ full_name }}
{%- endif %}

{%- if select is undefined or select is none %}
{%- if columns is defined and columns|length > 0 and all_columns_have_type %}
(

{%- for col_def in columns %}
    {{ col_def['name'] }} {{ col_def['type'] }}
    {{- ' UNIQUE' if col_def.get('unique')}}
    {{- ' NOT NULL' if col_def.get('not_null')}}
    {{- ',' if not loop.last else ''}}
{%- endfor %}
)
{%- endif %}
{%- endif %}

{%- block table_attributes %}
{%- if partition is defined and partition is not none %}
PARTITION BY {{ partition }}
{% endif %}

{%- if cluster is defined and cluster is not none %}
CLUSTER BY {{ cluster|join(', ') }}
{% endif %}

{%- if distribution is defined and distribution is not none %}
DISTSTYLE {{ distribution['type'] }}
{% if distribution['type'] == 'KEY' %}DISTKEY({{ distribution['column'] }}){% endif %}
{% endif %}

{%- if sorting is defined and sorting is not none %}
{{ sorting['type']+' ' if sorting['type']  else '' }}SORTKEY({{ sorting['columns']|join(', ') }})
{% endif %}
{% endblock -%}

{%- if temporary %}
DATA_RETENTION_TIME_IN_DAYS = 0
{% endif -%}

{%- if select is defined and select is not none %}

AS
{{ select }}

{% endif -%}
;

{% block indexes %}
{% if indexes is defined %}
  {% for name, idx_def in indexes.items() %}
CREATE INDEX {{ table_name }}_{{ name }} ON {{ full_name }}({{ ', '.join(idx_def['columns']) }});
  {% endfor %}
{% endif %}
{% endblock %}

{% block permissions %}
{% if permissions is defined %}
  {% for role, priv in permissions.items() %}
GRANT {{ priv }} ON {{ full_name }} TO {{ role }};
  {% endfor %}
{% endif %}
{% endblock %}

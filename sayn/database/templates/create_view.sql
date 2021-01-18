{% if not can_replace_view %}
{% if table_exists %}
DROP TABLE IF EXISTS {{ table_name }}{{ ' CASCADE' if needs_cascade else ''}};
{% elif view_exists %}
DROP VIEW IF EXISTS {{ table_name }}{{ ' CASCADE' if needs_cascade else ''}};
{% endif %}

CREATE VIEW {{ table_name }}
{%- else %}
CREATE OR REPLACE VIEW
{%- endif %}

{%- if select is defined %}
AS
  {%- if columns is defined %}
SELECT {{ columns|join('\n     , ', attribute='name') }}
  FROM ({{ select }}) t
  {%- else %}
{{ select }}
  {%- endif %}
{% endif -%}
;

{% block permissions %}
{% if permissions is defined %}
  {% for role, priv in permissions.items() %}
GRANT {{ priv }} ON {{ table_name }} TO {{ role }};
  {% endfor %}
{% endif %}
{% endblock %}

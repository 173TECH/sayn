{% if can_replace_view and not table_exists %}
CREATE OR REPLACE VIEW {{ table_name }}
{%- else %}
{% if table_exists %}
DROP TABLE IF EXISTS {{ table_name }}{{ ' CASCADE' if needs_cascade else ''}};
{% elif view_exists %}
DROP VIEW IF EXISTS {{ table_name }}{{ ' CASCADE' if needs_cascade else ''}};
{% endif %}

CREATE VIEW {{ table_name }}

{%- endif %}

{%- if select is defined %}
AS
{{ select }}
{% endif -%}
;

{% block permissions %}
{% if permissions is defined %}
  {% for role, priv in permissions.items() %}
GRANT {{ priv }} ON {{ table_name }} TO {{ role }};
  {% endfor %}
{% endif %}
{% endblock %}

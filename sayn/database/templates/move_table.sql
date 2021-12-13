{% set src_prefix = src_schema+'.' if src_schema else '' -%}
{% set dst_prefix = dst_schema+'.' if dst_schema else '' %}

{% if view_exists%}
DROP VIEW IF EXISTS {{ dst_prefix }}{{ dst_table }}{{ ' CASCADE' if needs_cascade else ''}};
{% else %}
DROP TABLE IF EXISTS {{ dst_prefix }}{{ dst_table }}{{ ' CASCADE' if needs_cascade else ''}};
{% endif %}
{% if rename_changes_schema %}
ALTER TABLE {{ src_prefix }}{{ src_table }} RENAME TO {{ dst_prefix }}{{ dst_table }};
{% else %}
ALTER TABLE {{ src_prefix }}{{ src_table }} RENAME TO {{ dst_table }};
  {% if dst_schema != src_schema %}
ALTER TABLE {{ src_prefix }}{{ dst_table }} SET SCHEMA {{ dst_schema }};
  {% endif %}
{% endif %}

{% if indexes is defined %}
  {% for name, idx_def in indexes.items() %}
    {% if not cannot_alter_indexes %}
ALTER INDEX {{ dst_prefix }}{{ src_table }}_{{ name }} RENAME TO {{ dst_table }}_{{ name }};
    {% else %}
DROP INDEX IF EXISTS {{ dst_prefix }}{{ src_table }}_{{ name }};
CREATE INDEX {{ dst_table }}_{{ name }} ON {{ dst_prefix }}{{ dst_table }}({{ ', '.join(idx_def['columns']) }});
    {% endif %}
  {% endfor %}
{% endif %}

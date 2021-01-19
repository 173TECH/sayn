{% set src_schema = src_schema+'.' if src_schema else '' -%}
{% set dst_schema = dst_schema+'.' if dst_schema else '' %}

DROP TABLE IF EXISTS {{ dst_schema }}{{ dst_table }};
ALTER TABLE {{ src_schema }}{{ src_table }} RENAME TO {{ dst_table }};
{% if dst_schema != src_schema %}
ALTER TABLE {{ src_schema }}.{{ dst_table }};
{% endif %}

{% if indexes is defined %}
  {% for name, idx_def in indexes.items() %}
    {% if can_alter_indexes %}
ALTER INDEX {{ dst_schema }}{{ src_table }}_{{ name }} RENAME TO {{ dst_schema }}{{ dst_table }}_{{ name }};
    {% else %}
DROP INDEX IF EXISTS {{ dst_schema }}{{ src_table }}_{{ name }};
CREATE INDEX {{ dst_table }}_{{ name }} ON {{ dst_schema }}{{ dst_table }}({{ ', '.join(idx_def['columns']) }});
    {% endif %}
  {% endfor %}
{% endif %}

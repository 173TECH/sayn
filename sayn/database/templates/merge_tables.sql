DELETE FROM {{ dst_table }}
 WHERE EXISTS (SELECT *
                 FROM {{ src_table }}
                WHERE {{ src_table }}.{{ delete_key }} = {{ dst_table }}.{{ delete_key }});

INSERT INTO {{ dst_table }} SELECT * FROM {{ src_table }};

{% if cleanup is defined and cleanup %}
DROP TABLE {{ src_table }};
{% endif %}

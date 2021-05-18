DELETE FROM {{ dst_table }} t
 WHERE EXISTS (SELECT *
                 FROM {{ src_table }} s
                WHERE s.{{ delete_key }} = t.{{ delete_key }});

INSERT INTO {{ dst_table }} SELECT * FROM {{ src_table }};

{% if cleanup is defined and cleanup %}
DROP TABLE {{ src_table }};
{% endif %}

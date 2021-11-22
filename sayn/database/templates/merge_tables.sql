{% if delete_key is not none %}
DELETE FROM {{ dst_table }} AS t
 WHERE EXISTS (SELECT *
                 FROM {{ src_table }} AS s
                WHERE s.{{ delete_key }} = t.{{ delete_key }});
{% endif %}

INSERT INTO {{ dst_table }} SELECT * FROM {{ src_table }};

{% if cleanup is defined and cleanup %}
DROP TABLE {{ src_table }};
{% endif %}

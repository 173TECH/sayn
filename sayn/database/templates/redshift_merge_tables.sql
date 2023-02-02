{% if delete_key is not none %}
DELETE FROM {{ dst_table }}
 WHERE {{dst_table}}.{{ delete_key }} IN( SELECT {{ src_table }}.{{ delete_key }}
                                            FROM {{ src_table }});
{% endif %}

INSERT INTO {{ dst_table }} SELECT * FROM {{ src_table }};

{% if cleanup is defined and cleanup %}
DROP TABLE {{ src_table }};
{% endif %}

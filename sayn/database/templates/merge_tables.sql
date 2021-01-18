DELETE FROM {{ dst_table }} t
 WHERE EXISTS (SELECT *
                 FROM {{ src_table }} s
                WHERE {{ src_table }}.{{ delete_key }} = {{ dst_table }}.{{ delete_key }});

INSERT INTO {{ dst_table }} SELECT * FROM {{ src_table }};

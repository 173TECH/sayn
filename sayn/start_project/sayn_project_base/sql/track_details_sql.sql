/*
In this sql query, you can see the usage of parameters.
This is based on Jinja templating.
Parameters are passed from the profile used at run time (profiles are defined in the settings)
After a query is compiled, they will appear in the compile folder
*/

DROP TABLE IF EXISTS {{schema_models}}.{{table_prefix}}track_details_sql
;

CREATE TABLE {{schema_models}}.{{table_prefix}}track_details_sql AS

SELECT t.trackid
     , t.name
     , al.title album_name
     , ar.name artist_name

FROM {{schema_logs}}.tracks t

INNER JOIN {{schema_logs}}.albums al
  ON t.albumid = al.albumid

INNER JOIN {{schema_logs}}.artists ar
  ON al.artistid = ar.artistid
;

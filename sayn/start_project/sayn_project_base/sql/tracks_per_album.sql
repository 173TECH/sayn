/*
In this sql query, you can see the usage of parameters.
This is based on Jinja templating.
Parameters are passed from the profile used at run time (profiles are defined in the settings)
After a query is compiled, they will appear in the compile folder
*/

SELECT al.title album_name
     , COUNT(DISTINCT t.trackid) n_tracks

FROM {{schema_logs}}.tracks t

INNER JOIN {{schema_logs}}.albums al
  ON t.albumid = al.albumid

GROUP BY 1

/*
In this sql query, you can see the usage of parameters.
This is based on Jinja templating.
Parameters are passed from the profile used at run time (profiles are defined in the settings)
After a query is compiled, they will appear in the compile folder
*/

SELECT tpa.*

FROM {{schema_models}}.{{table_prefix}}tracks_per_album tpa --here we prepend the table name with {{table_prefix}} which enables to separate when testing. If ran from prod, there is no prefix. If a test user has prefix specified, then the prefix will be added.

ORDER BY 2 DESC

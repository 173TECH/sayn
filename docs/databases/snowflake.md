# Snowflake

SAYN will consider the following parameters to construct the sqlalchemy url:

Parameter  | Description                         | Default
---------  | ----------------------------------- | ---------------------
account    | account.region                      | Required
user       | User name used to connect           | Required
password   | Password for that user              | Required
database   | Database in use upon connection     | Required
role       | User role to use on this connection | Default role for user
warehouse  | Warehouse to use to run queries     | Default warehouse for user
schema     | Default schema for the connection   | 

Other parameters specified will be passed on to
[sqlalchemy.create_engine](https://docs.sqlalchemy.org/en/13/core/engines.html#sqlalchemy.create_engine)
when creating the engine.

!!! example "settings.yaml"
    ```yaml
    credentials:
      snowflake-conn:
        type: snowflake
        account: xy12345.us-east-1
        user: snowflake_user
        role: etl
        password: 'Pas$w0rd' #use quotes to avoid conflict with special characters
        database: models
        warehouse: etl-warehouse
    ```

Check the sqlalchemy [snowflake dialect](https://docs.snowflake.com/en/user-guide/sqlalchemy.html)
for extra parameters.

# PostgreSQL

The PostgreSQL connector looks for the following parameters in the credentials settings:

Parameter  | Description                           | Default
---------  | ------------------------------------- | --------
host       | Host name or public IP of the server  | Required
port       | Connection port                       | 5432
user       | User name used to connect             | Required
password   | Password for that user                | Required
dbname     | Database in use upon connection       | Required

Other parameters specified will be passed to
[sqlalchemy.create_engine](https://docs.sqlalchemy.org/en/13/core/engines.html#sqlalchemy.create_engine)
when creating the engine.

!!! example "settings.yaml"
    ```yaml
    credentials:
      postgresql-conn:
        type: postgresql
        host: warehouse.company.com
        port: 5432
        user: pg_user
        password: 'Pas$w0rd' #use quotes to avoid conflict with special characters
        dbname: models
    ```

Check the sqlalchemy [psycopg2](https://docs.sqlalchemy.org/en/13/dialects/postgresql.html#module-sqlalchemy.dialects.postgresql.psycopg2)
dialect for extra parameters.

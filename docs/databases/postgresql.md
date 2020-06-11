# PostgreSQL

SAYN will consider the following parameters to construct the sqlalchemy url:

- **host**
- **user**
- **password**
- **port**
- **dbname**

Other parameters specified will be passed on to 
[sqlalchemy.create_engine](https://docs.sqlalchemy.org/en/13/core/engines.html#sqlalchemy.create_engine)
when creating the engine.

!!! example "settings.yaml"
    ```yaml
    ...
    
    credentials:
      postgresql-conn:
        type: postgresql
        host: [host]
        port: [port]
        user: [username]
        password: '[password]' #use quotes to avoid conflict with special characters
        dbname: [database_name]
    
    ...
    ```

Check the sqlalchemy [psycopg2](https://docs.sqlalchemy.org/en/13/dialects/postgresql.html#module-sqlalchemy.dialects.postgresql.psycopg2)
dialect for extra parameters.

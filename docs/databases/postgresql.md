# PostgreSQL

SAYN will consider the following parameters to construct the sqlalchemy connection string, passing
the rest to [sqlalchemy.create_engine](https://docs.sqlalchemy.org/en/13/core/engines.html#sqlalchemy.create_engine):

- **host**
- **user**
- **password**
- **port**
- **dbname**

Example:

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

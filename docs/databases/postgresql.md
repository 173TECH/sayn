# PostgreSQL

## Connection

This is an example of PostgreSQL credential details to connect:

**`settings.yaml`**

```yaml
# ...

credentials:
  postgresql-conn:
    type: postgresql
    connect_args:
      host: [host]
      port: [port]
      user: [username]
      password: '[password]' #use quotes to avoid conflict with special characters
      dbname: [database_name]

# ...
```

The `connect_args` need to match the [sqlalchemy create_engine connect_args](https://www.psycopg.org/docs/module.html#psycopg2.connect).

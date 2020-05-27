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

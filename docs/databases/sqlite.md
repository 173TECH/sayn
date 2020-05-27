# SQLite

## Connection

This is an example of SQLite credential details to connect:

**`settings.yaml`**

```yaml
# ...

credentials:
  mysql-conn:
    type: mysql
    connect_args:
      host: [host]
      port: [port]
      user: [username]
      password: '[password]' #use quotes to avoid conflict with special characters
      database: [database]

# ...
```

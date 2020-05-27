# Redshift

## Connection

This is an example of Redshift credential details to connect:

**`settings.yaml`**

```yaml
# ...

credentials:
  redshift-conn:
    type: postgresql
    connect_args:
      host: [host]
      port: [port]
      user: [username]
      password: '[password]' #use quotes to avoid conflict with special characters
      dbname: [database_name]

# ...
```

# Snowflake

## Connection

This is an example of Snowflake credential details to connect:

**`settings.yaml`**

```yaml
# ...

credentials:
  snowflake-conn:
    type: snowflake
    connect_args:
      account: [account]
      user: [username]
      password: '[password]' #use quotes to avoid conflict with special characters
      database: [database]
      schema: [schema]
      warehouse: [warehouse]
      role: [role]

# ...
```

## Additional Notes

### Autocommit

Autocommit is False by default when creating Snowflake connections through sqlalchemy. If you are using `sql` tasks, you might want to set the `autocommit` attribute to True in the connection credentials.

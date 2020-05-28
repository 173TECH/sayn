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

The `connect_args` need to match the [sqlalchemy create_engine connect_args](https://docs.snowflake.com/en/user-guide/python-connector-example.html#connecting-to-snowflake).

## Additional Notes

### Autocommit

Autocommit is False by default when creating Snowflake connections through sqlalchemy. If you are using `sql` tasks, you might want to set the `autocommit` attribute to True in the connection credentials.

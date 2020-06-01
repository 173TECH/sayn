# SQLite

## Connection

This is an example of SQLite credential details to connect:

**`settings.yaml`**

```yaml
# ...

credentials:
  sqlite-conn:
    type: sqlite
    database: [path_to_database]

# ...
```

The attributes other than `type` need to match the [sqlalchemy create_engine connect_args](https://docs.python.org/3/library/sqlite3.html#sqlite3.connect).

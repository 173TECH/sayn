# SQLite

SAYN will form the sqlalchemy connection URL with the `database` parameter,
which should point to a file path relative to the SAYN project.

Any parameter other than `database` will be passed to
[sqlalchemy.create_engine](https://docs.sqlalchemy.org/en/13/core/engines.html#sqlalchemy.create_engine){target="\_blank"}.

!!! example "settings.yaml"
    ```yaml
    credentials:
      sqlite-conn:
        type: sqlite
        database: [path_to_database]
    ```


Check the sqlalchemy [SQLite](https://docs.sqlalchemy.org/en/13/dialects/sqlite.html){target="\_blank"}
dialect for extra parameters.

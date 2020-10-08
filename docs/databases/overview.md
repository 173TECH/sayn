# Databases

## About

SAYN uses [sqlalchemy](https://www.sqlalchemy.org/){target="\_blank"} in order to manage database connections. It currently supports the following databases:

* [Redshift](redshift.md)
* [Snowflake](snowflake.md)
* [PostgreSQL](postgresql.md)
* [Mysql](mysql.md)
* [SQLite](sqlite.md)

## Usage

Database connections are defined as `credentials` in `settings.yaml` by specifying the database `type` and other connection parameters.

!!! example "settings.yaml"
    ```yaml
    credentials:
      db_name:
        type: redshift

        host: ...
        # other connection parameters ...
    ```

You can check the list of connection parameters in the database specific pages of this section. If a
parameter that is not listed on the database page is included in `settings.yaml`, that parameter will be passed to
[sqlalchemy.create_engine](https://docs.sqlalchemy.org/en/13/core/engines.html#sqlalchemy.create_engine). Refer to sqlalchemy's documentation if you need to fine tune the connection.

For example, to specify a connection timeout for a PostgreSQL connection, this can be specified in the `connect_args` parameter:

!!! example "settings.yaml"
    ```yaml
    credentials:
      db_name:
        type: postgresql

        host: ...
        # other connection parameters

        connect_args:
          connect_timeout: 100
    ```

## Using databases in Python tasks

Databases defined in the SAYN project are available to Python tasks via `Config.dbs`. For
convenience though, all Python tasks have a `default_db`.

!!! example "Example PythonTask"
    ``` python hl_lines="8"
    from sayn import PythonTask

    class TaskPython(PythonTask):
        def setup(self):
            #code doing setup

        def run(self):
            data = self.default_db.select("SELECT * FROM test_table")
            #code you want to run
    ```

## Database class

::: sayn.database.Database
    selection:
      members:
        - execute
        - select
        - load_data

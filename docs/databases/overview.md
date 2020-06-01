# Databases

## About

SAYN uses [sqlalchemy](https://www.sqlalchemy.org/){target="\_blank"} in order to manage database connections. It currently supports the following databases:

* [PostgreSQL](postgresql.md)
* [Snowflake](snowflake.md)
* [SQLite](sqlite.md)

## Usage

In order to connect to databases, the connection credentials need to be added into the `credentials` sections of the `settings.yaml` file. Database credentials need to specify two key attributes:

* `type`: the connection type. This needs to be one of the supported databases.
* `connect_args`: the connection arguments. Because SAYN uses sqlalchemy, those `connect_args` match the `connect_args` from the sqlachemy `create_engine` method.

Please see the database specific pages to see credential examples for each database.

## SAYN Database API

SAYN provides three core functions in order to interact with the default database in Python scripts. The default database is stored on the `default_db` attribute of the Task object. It can therefore be accessed as follows in any Python task:

```python
from sayn import PythonTask

class TaskPython(PythonTask):
    def setup(self):
        #code doing setup

    def run(self):
        db = self.default_db
        #code you want to run
```

Please see below the details of the available methods on the `default_db`.

### `.execute()`

#### Functionality

Lets you execute any SQL script on the default database.

#### Signature

`def execute(self, script)`

* `script`: the SQL script to be executed. There can be multiple SQL queries separated by `;` within a single script.

#### Returns

None

### `.select()`

#### Functionality

Runs a SQL query and returns the results in a list of tuples.

#### Signature

`def select(self, query, params=None)`

* `query`: the SQL script to be executed.
* `params`: parameters of the `sqlalchemy` `execute` method.

#### Returns

List of tuples containing the results. There is one tuple per row.

### `.load()`

#### Functionality

Loads data into a destination table.

#### Signature

`def load_data(self, table, schema, data)`

* `table`: the destination table.
* `schema`: the destination schema.
* `data`: the data to be loaded. It should be a list of tuples, with one tuple per row.

#### Returns

None.

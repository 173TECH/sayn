# Data Tests

## About

SAYN provides an extension to the functionality of the `columns` field in task definitions that enables the user to make use of predefined (standard) or custom tests for their table data fields.

Standard tests are implemented for the `autosql` and `copy` task types, while custom tests are not task bound and can be applied to any table in the warehouse. Custom tests work like SAYN `sql` tasks with a specific SQL query structure that only execute during the SAYN test suite.

Running `sayn test` through the CLI will execute the standard and custom tests for a given project.

All CLI usage for tasks applies to tests as well, with the major difference being that tests don't make use of a DAG to determine the order of execution (so attempting to execute tests on parents of children of a task is not supported).

Examples:

* `sayn test -t test_name`: run `task_name` only.
* `sayn test -t test1 test2`: runs `task1` and `task2` only.
* `sayn test -x test_name`: run all tests except `task_name`.


## Test Types

Please see below the available SAYN test types:

- [`unique`](standard.md): is applied on any number of columns of a given table and is responsible for validating uniqueness. This will also define constraints during the table creation where applicable.
- [`not_null`](standard.md): is applied on any number of columns of a given table and is responsible for validating nullity (or rather the lack of it). This will also define constraints during the table creation where applicable.
- [`allowed_values`](standard.md): is applied on any number of columns of a given table and is responsible for validating accepted values. This will also define constraints during the table creation where applicable.
- [`custom`](custom.md): is a specifically formated SQL query whose output is used to validate a successful or failed test.


## Defining Tests

Tests are defined in a list format using the `tests` subfield for each entry in `columns`. For `unique` and `not_null` you need to include these keywords in the list, while for `allowed_values` we define another list that is populated by the allowed string values for that data field.

For example, we can define tests to verify uniqueness and nullity for the `id` field and allowed values for the `alias` field for the following task in the `core` group:

!!! example "tasks.yaml"
    ```
      task:
        type: autosql
        file_name: "task.sql"
        materialisation: table
        destination:
          table: "{{ task.name }}"
        columns:
          - name: id
            tests:
              - unique
              - not_null
          - name: alias
            tests:
              - allowed_values:
                - 'first'
                - 'second'
                - 'third'
    ```
We can also define the tests inside `task.sql` by call `config` from a Jinja tag:

!!! example "tasks.sql"
    ```
    {{ config(columns=[ {'name': 'id', 'tests':['unique', 'not_null']},
                        {'name':'alias', 'tests':['allowed_values':['first','second','third']}]) }}


    SELECT ...
    ```


Custom tests are defined in their own task group called `tests` (defining tasks in an arbitrary `test` group will cause SAYN to fail). `custom` tests are provided with an SQL file that needs to exist in a `tests` folder in the `sql` project folder.

For example, we can define a custom tests that executes the test query presented bellow:

!!! example "tests.yaml"
    ```
    tests:
      test_1:
        file_name: test.sql

    tasks:
      ...
      ...
    ```

!!! example "SQL test query"
    ```
    SELECT l.arena_id
    FROM dim_arenas as l
    WHERE l.arena_id IS NULL
    GROUP BY l.arena_id
    HAVING COUNT(*) > 0
    ```


You can read more about test types by heading to the corresponding pages.

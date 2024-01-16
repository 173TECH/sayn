# `custom` tests

## About

Custom tests are defined in their own task group `tests`. In the test definition you provide an SQL query that comprises the test and during execution that query will get executed.

!!! info
    Defining normal tasks in the `tests` group will cause SAYN to fail. The SQL queries provided to the `custom` test, needs to live in a folder in the `sql` folder of the project.

## Defining Tests

Defining `custom` tests is quite straight-forward. You only need to provide a `folder_name/file_name`:

!!! example "tests.yaml"
    ```yaml
    tests:
      test_1:
        file_name: *folder_in_sql*/test.sql

    tasks:
      ...
      ...
    ```

Custom tests can also be defined as groups (just like with `sql` tasks). The syntax is the same as in the `sql` tasks, like so:

!!! example "project.yaml"
    ```yaml
    groups:
      tests:
        type: test
        file_name: "{{ task.group }}/*.sql"
    ```


## Writing `custom` test queries

SAYN considers a test to be successful (meaning it passed) when the executing query returns empty (with no results). Thus, when writing `custom` test queries, the test needs to be expressed as a lack of results to show. As an example, we can look at how `unique` and `not_null` tests can be implemented with `custom` tests:

!!! example "SQL test query - unique"
    ```sql
    SELECT t.column
         , COUNT(*)
      FROM table t
     GROUP BY t.columns
    HAVING COUNT(*) > 1
    ```

!!! example "SQL test query - nullity"
    ```sql
    SELECT t.column
         , COUNT(*)
      FROM table t
     WHERE t.column IS NULL
     GROUP BY t.column
    HAVING COUNT(*) > 1
    ```

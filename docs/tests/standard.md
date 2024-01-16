# `standard` tests

## About

Standard tests correspond to tests on nullity, uniqueness and accepted values. They are supported for `autosql` and `copy` tasks.

## Defining `standard` Tests

Standard tests are defined in a list format using the `tests` subfield for each entry in `columns`. The attributes that can be added to the list are as follows:

* `unique`: execute a uniqueness test on the column.
* `not_null`: execute a not nullity test on the column.
* `allowed_values`: execute a check on whether the column contains ONLY the allowed values (provided in a list).
* `execute`: will execute the test if `True` will skip if `False`.

!!! info
    `execute` is a field that needs to exist in the same list level as the test name. In that case, instead of defining the test with the string of the test type, you will need to define the test type with the `name` attribute and bellow it define the `execute` attribute.

An example of standard tests being defined for a task is:
!!! example "tasks.yaml"
    ```yaml
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
            - name: not_null
              execute: True
        - name: alias
          tests:
            - name: allowed_values
              - 'first'
              - 'second'
              - 'third'
              execute: False
    ```
We can also define the tests inside `task.sql` by call `config` from a Jinja tag:

!!! example "tasks.sql"
    ```sql
    {{ config(columns=[ {'name': 'id', 
                        'tests':['unique', {'name':'not_null', 'execute':True}]},

                        {'name':'alias', 
                        'tests':[{'name':'allowed_values':['first','second','third'], execute: False }]}
                      ]
        ) 
    }}

    SELECT ...
    ```

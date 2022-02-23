# Database objects

The most common task found in SAYN projects is the `autosql` task. This is a type of tasks where you write a `SELECT` statement
and SAYN handles the database object creation. When you run the project you would want a task selecting from a table to run
after the task that creates said table. In order to simplify task dependencies, SAYN considers database objects core concepts
and provides some tools to treat them as such.

## Object specification

In a relational database we typically find database tables organised into schemas, so for example `logs.tournaments` refers to
a table (or view) called `tournaments` in the `logs` schema, whereas `arenas` refers to a table (or view) called `arenas` in the
default schema. SAYN uses the same format to refer to database tables and views, but as we'll see this allows for a more dynamic use.

## Compilation of object names

In a real world scenario we want to write our code in a way that dynamically changes depending on the profile we're running
on (eg: test vs production). This allows for multiple people to collaborate on the same project, wihtout someone's actions
affecting the work of others in the team. Let's consider this example task:

!!! example "tasks/core.yaml"
    ```
    tasks:
      example_task:
        type: autosql
        materialisation: table
        file_name: example_task.sql
        destination:
          schema: models
          table: example_model
    ```

This task uses the `SELECT` statement in the file `sql/example_task.sql` and creates a table `example_model` in the `models` schema
of the default database. Now, if someone in your team runs this task, `models.example_model` will be replace with their new code
and if someone else in the team is executing a task that reads from it it can produce undesired results.

A way to solve this problem could be to have different databases for each person in a team but that can easily lead to complicated
database setups, potential data governance issues and increased database costs, as you might need a copy of the data per person
working with it.

In SAYN there's another solution: we express database object names like `schema.table` but the code that's execution in the database
is transformed according to personal settings. For example, we could have a schema called `analytics_models` where our production lives
and another called `test_models` where we store data produced during development, with table names like `USER_PREFIX_table` rather
than `table` so there's no collision and we minimise data redundancy.

!!! warning
    Name configuration only affects the `default_db`. When a SAYN project has more than 1 database
    connection you can still use the macros described in this page to set dependencies, but the
    resulting value of the macro is exactly the same as the input.

## Name configuration

The modifications described above are setup with prefixes, suffixes and overrides. For example:

!!! example "settings.yaml"
    ```
    profiles:
      test:
        schema_prefix: test
        table_prefix: up
    ```

The above will make every `schema.table` specification to be compiled to `test_schema.up_table`.

Following the example in the previous section, if we want the to call the production schema `analytics_models` we can do so by
adding the prefix in the `project.yaml` file:

!!! example "project.yaml"
    ```
    schema_prefix: analytics
    ```

Having both files above configured like that will make it so that referencing `models.example_model` on production will be translated
to `analytics_models.example_model` whereas the same code during testing will be translated as `test_models.up_example_model`. In other
words, what we define in `project.yaml` is the default behaviour which can be overriden in `settings.yaml`.

Aside from `schema_prefix` and `table_prefix` we also have `suffix` (`schema_suffix` and `table_suffix`) which as expected would instead
of prepending the value and an underscore, it adds that at the end.

!!! info
    Although the name of these settigns is `table_*` this also applies to views in the database. Similarly
    in some databases the concept of `schema` is called differently (eg: dataset in BigQuery) but `schema`
    is still used for all databases in SAYN.

## Referencing database objects

So far we've seen how the properties of an autosql task use this database object specification, but the real power of this feature is when
used in the code of the task itself, which we do this with the `src` and `out` macros. For example:

!!! example "settings.yaml"
    ```
    profiles:
      test:
        schema_prefix: test
        table_prefix: up
    ```

!!! example "sql/example_model.sql"
    ```
    SELECT *
      FROM {{ src('logs.raw_table') }}
    ```

Here we're calling the `src` macro that does 2 things:

  * Using prefixes and suffixes translates `logs.raw_table` to the appropriate table name
  * Declares that `example_task` (as defined earlier in this page) depends on the task(s) that produce `logs.raw_table`

So the output code to be executed in the database will be:

!!! example "compile/core/example_model.sql"
    ```
    -- SAYN adds the table management code
    SELECT *
      FROM test_logs.up_raw_table
    ```

The counterpart to `src` is `out` which similarly translates the value to the appropriate database name, as well as it defines database
objects produced by the task. In `autosql` tasks `out` is not present since there's no usage for it, however this is useful for `sql` tasks:

!!! example "sql/example_sql.sql"
    ```
    CREATE OR REPLACE TABLE {{ out('models.sql_example') }} AS
    SELECT *
      FROM {{ src('logs.raw_table') }}
    ```

This code tells SAYN that this sql task produces the table `models.sql_example` and depends on the table `logs.raw_table`, while
simultaneously producing this example code to be executed in the database:

!!! example "compile/core/example_sql.sql"
    ```
    CREATE OR REPLACE TABLE test_models.up_sql_example AS
    SELECT *
      FROM test_logs.up_raw_table
    ```

`src` and `out` are also available to python tasks, however we use them with `context.src` or `self.src`:

!!! example "python/example.py"
    ```
    @task(sources='logs.raw_table')
    def example_python(context, warehouse):
        table_name = context.src('logs.raw_table')
        data = warehouse.read_data(f"select * from {table_name}")
        ...
    ```

!!! example "python/advanced_example.py"
    ```
    class MyTask(PythonTask):
        def config(self):
            self.table_name = self.src('logs.raw_table')

        def run(self):
            data = self.default_db.read_data(f"select * from {self.table_name}")
        ...
    ```

The above examples are equivalent to each other and we use `context.src` in the decorator form and `self.src` in the more advanced class
model. `context.out` and `self.out` are also available in python tasks and their behaviour is the same as with sql and autosql tasks.

!!! info
    `src` should only be used for tables that are managed by the SAYN project. If an external EL tool is being used to load data
    into your warehouse, references to these tables should be hardcoded instead, as their names never change depending on your SAYN
    profile, nor there are any task dependencies to infer from using `src`.

Note that calling `src` and `out` in the `run` method of a python task class or in the function code when using a decorator doesn't
affect task dependencies, it simply outputs the translated database object name. The task dependency behaviour in python tasks is done
by either calling `self.src` or `self.out` in the `config` method of the class or by passing these references to the `task` decorator
in the `sources` and `outputs` arguments as seen in this example. For more details head to [the python task section](tasks/python).

## Altering the behaviour of `src`

A very common situation when working in your data pipeline is when we have a lot of data to work with but at any point in time while modelling
we find ourselves working only a subset of it. Working with sample data can be inconvenient during development because it hinders our
ability to evaluate the result and the alternative, having a duplicate of the data for every person in the team, can be costly both in
terms of money and time producing and maintaining these duplicates. For this reason SAYN comes equiped with 2 features that simplifies this
switchin: `from_prod` and upstream prod.

`from_prod` is most useful when a team member never deals with a part of the SAYN project. For example, a data analyst that only deals with 
modelling tasks in a SAYN project that also has extraction tasks. Upstream prod is most useful when we're doing changes to a small set of task,
so we don't want to have to repopulate all the upstream tables.

### `from_prod` configuration

The first mechanism is `from_prod` which we set in the `settings.yaml` file and override the behaviour of `src`. An example:

!!! example "project.yaml"
    ```
    schema_prefix: analytics
    ```

!!! example "sql/core/test_table.sql"
    ```
    SELECT *
      FROM {{ src('logs.extract_table') }}
    ```

!!! example "settings.yaml"
    ```
    profiles:
      dev:
        table_prefix: up
        schema_prefix: test
        from_prod:
          - "logs.*"
    ```

In the above example we have a task selecting data from `logs.extract_table` which for the purpose of this example we can assume is
created by an extraction task pulling data from an API. On production, `src('logs.extract_table')` will be translated as
`analytics_logs.extract_table`, whereas during development it will be translated as `test_logs.up_extract_table`, given the
configuration in the `dev` profile in `settings.yaml`. However there's also a `from_prod` entry with `logs.*` which is telling
SAYN that all tables or views from the `logs` schema should come from production, so the final code for the `test_table` task will
actually be:

!!! example "compile/core/test_table_select.sql"
    ```
    SELECT *
      FROM analytics_logs.extract_table
    ```

As you can see, we just need to specify a list of tables in `from_prod` to always read from the production configuration, that is, the
settings shared by all team members as specified in `project.yaml`. To make it easier to use, wildcards (`*`) are accepted, so that we
can specify a whole schema like in the example, but we can also specify a list of tables explicitely instead.

`from_prod` can also be specified using environment variables with `export SAYN_FROM_PROD="logs.*"` where the value is a comma
separated list of tables.

!!! warning
    To avoid accidentally affecting production tables, `from_prod` only affects `src`. The result of calling `out` always evaluate
    to your configuration in `settings.yaml` or environment variables.

### Upstream prod

The second mechanism to override the behaviour of `src` is upstream prod. We use upstream prod by specifying the flag (`-u` or `--upstream-prod`)
when running SAYN while filtering, for example `sayn run -t example_task -u`. When we do this, any reference to tables produced by tasks not
present in the current execution will use the parameters defined in `project.yaml`.
For example:


!!! example "project.yaml"
    ```
    schema_prefix: analytics
    ```

!!! example "settings.yaml"
    ```
    profiles:
      test:
        schema_prefix: test
        table_prefix: up
    ```

!!! example "sql/example_model.sql"
    ```
    SELECT *
      FROM {{ src('logs.raw_table') }}
    ```

Running `sayn run -t example_task` will run the following code in the database:

!!! example "compile/core/example_task_create_table.sql"
    ```
    CREATE OR REPLACE TABLE test_models.up_example_model AS
    SELECT *
      FROM test_logs.up_raw_table
    ```

So the `src` macro translates `logs.raw_table` to the testing name `test_logs.up_raw_table`. However, with upstream prod
(`sayn run -t example_task -u`) the code executed will be:

!!! example "compile/core/example_task_create_table.sql"
    ```
    CREATE OR REPLACE TABLE test_models.up_example_model AS
    SELECT *
      FROM analytics_logs.raw_table
    ```

Since no task in this execution creates `logs.raw_table` in SAYN translates that instead to the production name `analytics_logs.raw_table`,
while the table created is still the test version.

Let' assume now that we have another task that we want to include in the execution:

!!! example "sql/another_example_model.sql"
    ```
    SELECT *
      FROM {{ src('models.example_model') }}
    ```

So when run `sayn run -t example_task another_example_task -u` the code for the `example_task` will remain the same as above,
but the code executed for `another_example_model` will be:

!!! example "compile/core/another_example_task_create_table.sql"
    ```
    CREATE OR REPLACE TABLE test_models.up_another_example_model AS
    SELECT *
      FROM test_models.up_example_model
    ```

Because `example_task` is part of this exeuction and produces the table `models.example_model` reference by `another_example_task`
`models.example_model` is translated using the testing settings into `test_models.up_example_model` unlike `logs.raw_table` which
as no task producing it is present in this execution, will be translated into the production name.

With upstream prod it becomes a lot easier to work with your modelling layer without having to duplicate all your upstream tables
for every person in the team or being forced to work with sampled data.

## Advanced usage

For a more advanced usage, we also have `schema_override` and `table_override` which allows us to completely change the behaviour.
With `override` what we do is define the exact value that a schema or table name will have based on some Jinja template logic. To
this template 3 values are passed:

  * `table`: the name of the table specified in sayn code
  * `schema`: the name of the schema specified in sayn code
  * `connection`: the name of the connection it refers to

!!! example "settings.yaml"
    ```
    profiles:
      test:
        schema_override: "{% if schema != 'logs' %}test{% else %}analytics{% endif %}_{{ schema }}"
        table_override: "{% if schema != 'logs' %}up_{{ table }}{% else %}{{ table }}{% endif %}"
    ```

With this example, a reference to `models.example_model` will be translated as `test_models.up_example_model` but a reference to
`logs.raw_logs` will be translated as `analytics_logs.raw_logs`. This can be useful in cases where someone in the team never
works with data ingestion, so every modelling task ran by them will always reads from production data, rather than having to
duplicate the data or having to work with a sample of this raw data.

!!! warning
    Note that with the above example of override, a task writting to the logs schema will always write to the production version
    `analytics_logs` so to avoid issue you should always have good permissions setup in your database.

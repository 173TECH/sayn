# Tutorial: Part 2

This tutorial builds upon the [Tutorial: Part 1](tutorial_part1.md) and introduces
you to SAYN concepts which will enable you to make your projects more dynamic and efficient.

In [Tutorial: Part 1](tutorial_part1.md) we implemented our first ETL process with SAYN. We will now expand on that by adding `parameters` and `presets`.

A [Github repository](https://github.com/173TECH/sayn_tutorial_part2){target="\_blank"} is available with the final code if you prefer to have all the code written directly. After cloning, you simply need to make sure to rename the `sample_settings.yaml` file to `settings.yaml` for the project to work.

### Step 1: Define the project `parameters`

You can use `parameters` in order to make your SAYN tasks' code dynamic. We will set one project parameter called `user_prefix`. This will enable us to distinguish which user generated tables.

First, add `paramaters` add the end of `project.yaml`. This is a yaml map which defines the default
value.

!!! example "project.yaml"
    ``` yaml
    # ...
    parameters:
      user_prefix: '' #no prefix for prod
    ```

In this case we defined a parameter `user_prefix` that we will use to name tables. This is useful when multiple users are testing a project as it allows the final table name to be different between collaborators.

Now we can define the value of the parameter on each profile. We do this in the `settings.yaml`.

!!! example "settings.yaml"
    ``` yaml
    profiles:
      dev:
        credentials:
          warehouse: dev_db
        parameters:
          user_prefix: up_
      prod:
        credentials:
          warehouse: prod_db
    ```

Note how we don't redefine the parameter in our prod profile as the default value is more appropriate.

### Step 2: Making Tasks Dynamic With `parameters`

Now that our parameters are setup, we can use those to make our tasks' code dynamic.

#### In `python` tasks

For the Python `load_data` task, we will access the `user_prefix` parameter and then pass it to the
functions doing the data processing. You can look into `python/utils.py` to see how we use the `user_prefix` parameter to change the table names.

!!! example "python/load_data.py"
    ```python
        # ...
        def run(self):
            user_prefix = self.parameters['user_prefix']
        # ...
                    q_create = get_create_table(log_type, user_prefix)
        # ...
    ```

#### In `autosql` tasks

The files in the `sql` folder are always interpreted as [Jinja](https://palletsprojects.com/p/jinja/){target="\_blank"}
templates. This means that in order to access parameters all we have to do is enclose it in `{{ }}` Jinja blocks. For example, in order to reference the tables created by `load_data` the `dim_arenas` task can be changed like this:

!!! example "sql/dim_arenas.sql"
    ```sql
    SELECT l.arena_id
         , l.arena_name

    FROM {{user_prefix}}logs_arenas l
    ```

Now `sayn run` will transform the above into valid SQL creating `compile/base/dim_arenas.sql` with it. The file path following the rule `compile/task_group_name/autosql_task_name.sql`:

!!! example "compile/base/dim_arenas.sql"
    ```sql
    SELECT l.arena_id
         , l.arena_name

    FROM up_logs_arenas l
    ```

SAYN provides a `sayn compile` command that works like `sayn run` except that it won't execute the code. What it does though, is generate the compiled files that SAYN would run with the `sayn run` command.

### Step 3: Making task definitions dynamic with `parameters`

Now that our python task generates tables with the `user_prefix` in the name and our autosql tasks will select data from it. What we also need to do is change the table names our autosql tasks are generating. For that, let's take `dim_arenas` and modify it so that it generates a table called `up_dim_arenas` (or other user_prefix defined in `settings.yaml`):

!!! example "tasks/base.yaml"
    ```yaml
    tasks:
      # ...
      dim_arenas:
        type: autosql
        file_name: dim_arenas.sql
        materialisation: table
        destination:
          table: '{{ user_prefix }}{{ task.name }}'
        parents:
          - load_data
      # ...
    ```

Note the value of `destination.table` is now some Jinja code that will compile to the value of `user_prefix` followed by the name of the task.

### Step 4: Using `presets` to standardise task definitions

Because most of our tasks have a similar configuration, we can significantly reduce the YAML task definitions using `presets`. `presets` allow you to define common properties shared by several
tasks.

!!! example "tasks/base.yaml"
    ```yaml
    presets:
      modelling:
        type: autosql
        file_name: '{{ task.name }}.sql'
        materialisation: table
        destination:
          table: '{{ user_prefix }}{{ task.name }}'
        parents:
          - load_data

    tasks:
      # ...
      dim_arenas:
        preset: modelling
      # ...
    ```

Now the `modelling` preset has to dynamic properties:
* `table`: defined like we did in the previous so that the create table contains the `user_prefix` in the name.
* `file_name`: that uses the task name to point at the correct file in the sql folder.

In addition, `modelling` is defined so that tasks referencing it:
* are `autosql` tasks.
* Materialise as tables.
* Have `load_data` as a parent task, so that models always run after our log generator.

When a task references a preset, we're not restricted to the values defined in the preset. A task can override those values. Take `f_rankings` for example:

!!! example "tasks/base.yaml"
    ```yaml
    tasks:
      # ...
      f_rankings:
        preset: modelling
        materialisation: view
        parents:
          - f_fighter_results
    ```

Here we're overloading 2 properties:
* `materialisation` which will make f_rankings a view rather than a table.
* `parents` which will make `f_ranking` depend on `f_fighter_results` besides `load_data` as defined
  in the preset.

## Running Our New Project

You can now test running `sayn run` or `sayn -p prod`. The two options will do the following:

* `sayn run`:
    * use our `dev` profile
    * create all tables into a `dev.db` database
    * prefix all tables with `up_` and read from `up_` prefixed tables
* `sayn run -p prod`:
    * use our `prod` profile
    * create all tables into a `prod.db` database
    * will not use a prefix when creating / reading tables

## What Next?

This is it, you should now have a good understanding of the core ways of using SAYN. You can play further with this project and easily transfer it to a PostgreSQL database for example by:

* changing the credentials in `settings.yaml`.
* setting the `tmp_schema` and `schema` attributes of your `modelling` preset to `public`.

Otherwise, you can learn more about the specific SAYN features by having a look at the specific sections of the documentation.

Enjoy SAYN :)

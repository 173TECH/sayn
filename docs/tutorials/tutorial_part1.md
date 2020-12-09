# Tutorial: Part 1

This tutorial covers the basic concepts of SAYN and will get you going quickly. It uses the example project created by `sayn init`. It assumes SAYN is setup as described in the
[installation section](../installation.md).

This project generates some random data with a `python` task and performs some modelling on it with `autosql` tasks.

## Running SAYN

To get started, open a terminal, activate your virtual environment (`source sayn_venv/bin/activate`) and run the following:

```bash
sayn init sayn_tutorial
cd sayn_tutorial
sayn run
```

This will create a new project with the contents of this tutorial and execute it.

![`sayn run` execution](sayn_run1.gif)

You can open `dev.db` and see the tables and views created by `sayn run`. You can use
[DB Browser for SQLite](https://sqlitebrowser.org/dl/){target="\_blank"} in order to view the content of the database. As you can observe, `sayn run` created a small ETL process which models battles from various tournaments.

That's it, you made your first SAYN run! We will now explain what happens in the background.

## Project Overview

The `sayn_tutorial` folder has the following structure:

```
tutorial
├── project.yaml
├── settings.yaml
├── tasks
│   └── base.yaml
├── python
│   ├── __init__.py
│   └── load_data.py
├── sql
│   ├── dim_arenas.sql
│   ├── dim_fighters.sql
│   ├── dim_tournaments.sql
│   ├── f_battles.sql
│   ├── f_fighter_results.sql
│   └── f_rankings.sql
├── compile
├── .gitignore
├── readme.md
└── requirements.txt
```

The main files are:

* `project.yaml`: defines the SAYN project. It is **shared across all collaborators**.
* `settings.yaml`: defines the individual user's settings. It is **unique for each collaborator and should never be pushed to git** as it contains credentials.
* `tasks`: folder where the task files are stored. Each file is considered a task group.
* `python`: folder where scripts for `python` tasks are stored.
* `sql`: folder where SQL files for `sql` and `autosql` tasks are stored.
* `logs`: folder where SAYN logs are written.
* `compile`: folder where compiled SQL queries before execution.

## Implementing your project

Now let's see how the tutorial project would be created from scratch.

### Step 1: Define the project in `project.yaml`

The `project.yaml` file is at the root level of your directory and contains:

!!! example "project.yaml"
    ```yaml
    required_credentials:
      - warehouse

    default_db: warehouse
    ```

The following is defined:

* `required_credentials`: the list of credentials used by the project. In this case we have a single credential called `warehouse`. The connection details will be defined in `settings.yaml`.
* `default_db`: the database used by sql and autosql tasks. Since we only have 1 credential, this field could be skipped.

### Step 2: Define your individual settings with `settings.yaml`

The `settings.yaml` file at the root level of your directory and contains:

!!! example "settings.yaml"
    ```yaml
    profiles:
      dev:
        credentials:
          warehouse: dev_db
      prod:
        credentials:
          warehouse: prod_db

    default_profile: dev

    credentials:
      dev_db:
        type: sqlite
        database: dev.db
      prod_db:
        type: sqlite
        database: prod.db
    ```

The following is defined:

* `profiles`: the definion of profiles for the project. A profile defines the connection between credentials in the `project.yaml` file and credentials defined below. In this case we define 2 profiles dev and prod.
* `default_profile`: the profile used by default at execution time. It can be overriden using `sayn run -p prod`.
* `credentials`: here we define the credentials. In this case we have two for dev and prod, that are used as `warehouse` on each profile.

### Step 3: Define your tasks

In SAYN, tasks are defined in `yaml` files within the `tasks` folder. Each file is considered a [task group](../tasks/overview.md#task_groups). Our project contains only one task group: `base.yaml`:

!!! example "tasks/base.yaml"
    ```yaml
    tasks:
      load_data:
        type: python
        class: load_data.LoadData

      dim_tournaments:
        type: autosql
        file_name: dim_tournaments.sql
        materialisation: table
        destination:
          table: dim_tournaments
        parents:
          - load_data
    # ...
    ```

The `tasks` entry contains a map of tasks definitions. In this case we're using two types of tasks:

* `python`: lets you define a task written in Python. Python tasks are useful to complete your extraction and load layers if you're using an ELT tool or for data science models defined in Python.
* `autosql`: lets you write a `SELECT` statement while SAYN manages the table or view creation automatically for you. Our example has multiple `autosql` tasks which create models based on the logs.

!!! tip
    Although this tutorial only has one file in the `tasks` folder, you can separate tasks in multiple files. SAYN automatically includes any file from the `tasks` folder with a `.yaml` extension when creating the DAG. Each file is considered a [task group](../tasks/overview.md#task_groups).

#### `load_data` task

In our example project the only python task is `load_data` which creates some synthetic logs and loads them to our database. The code can be found in the class `LoadData` in `python/load_data.py`. Let's have a look at the main elements of a python task:

!!! example "python/load_data.py"
    ```python
    # ...
    from sayn import PythonTask

    class LoadData(PythonTask):
        def run(self):
            # Your code here
    ```

The above is the beginning of the python task. When the execution of `sayn run` hits the `load_data` task the code in the `run` method will execute.

A task in SAYN can be split into multiple steps, which is useful for debugging when errors occur. In this case, we first generate all data and then we load each dataset one by one. We can define the steps a task will follow with the `self.set_run_steps` method.

!!! example "python/load_data.py"
    ```python
        def run(self):
            # ...
            self.set_run_steps(
                [
                    "Generate Dimensions",
                    "Generate Battles",
                    "Load fighters",
                    "Load arenas",
                    "Load tournaments",
                    "Load battles",
                ]
            )
            # ...
    ```

To indicate SAYN what step is executing, we can use the following construct:

!!! example "python/load_data.py"
    ```python
        def run(self):
            # ...
            with self.step("Generate Dimensions"):
                # Add ids to the dimensions
                fighters = [
                    {"fighter_id": str(uuid4()), "fighter_name": val}
                    for id, val in enumerate(self.fighters)
                ]
                arenas = [
                    {"arena_id": str(uuid4()), "arena_name": val}
                    for id, val in enumerate(self.arenas)
                ]
                tournaments = [
                    {"tournament_id": str(uuid4()), "tournament_name": val}
                    for id, val in enumerate(self.tournaments)
                ]
    ```

Here our "Generate Dimensions" step simply generates the dimension variables with an id.

The final core element is accessing databases. In our project we defined a single credential called `warehouse` and we made this the `default_db`. To access this we just need to use `self.default_db`.

!!! example "python/load_data.py"
    ```python
    self.default_db.execute(q_create)
    ```

The main method in Database objects is `execute` which accepts a sql script via parameter and executes it in a transaction. Another method used in this tutorial is `load_data` which loads a dataset into the database automatically creating a table for it first.

For more information about how to build `python` tasks, visit the [python tasks section](../tasks/python.md).

#### Autosql tasks

Let's have a look at one of the autosql tasks (`dim_tournaments`). As you can see in `tasks/base.yaml`
above, we specify a `file_name` which contains:

!!! example "sql/dim_tournaments.yaml"
    ```sql
    SELECT l.tournament_id
         , l.tournament_name
    FROM logs_tournaments l
    ```

This is a simple `SELECT` statement that SAYN will use when creating a table called `dim_tournaments` as defined in the `destination` field in the `base.yaml` file.

For more information about setting up `autosql` tasks, visit the [autosql tasks section](../tasks/autosql.md).

## Running Your Project

So far we've used `sayn run` to execute our project, however SAYN provides more options:

* `sayn run -p [profile_name]`: runs the whole project with the specific profile. In our case using the profile `prod` will create a `prod.db` SQLite database and process all data there.
* `sayn run -t [task_name]`: allows the filtering the tasks to run.

More options are available to run specific components of your SAYN project. All details can be found in the [SAYN cli](../cli.md) section.

## What Next?

You now know the basics of SAYN, congratulations! You can continue learning by going through the [Tutorial: Part 2](tutorial_part2.md) which shows you several tricks to make your SAYN project more dynamic and efficient.

Otherwise, you can go through the rest of the documentation as SAYN has much more to offer!

Enjoy SAYN :)

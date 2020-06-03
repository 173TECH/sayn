# Tutorial: Part 1

## What We Will Cover

This tutorial covers the basic concepts of SAYN and will get you going quickly. It uses the example project in the folder created by `sayn init` and creates a small ETL process based on synthetic data.

If you need any help or want to ask a question, please reach out to the team at <sayn@173tech.com>.

## Running SAYN

Run the below in order to install SAYN and process your first SAYN run.

```bash
$ pip install sayn
$ sayn init test_sayn
$ cd test_sayn
$ sayn run
```

Running `sayn run` will output logging on your terminal about the process being executed. This is what will happen:

* SAYN will run all project tasks. Those are all in the DAG file `dags/base.yaml`.
* The tasks include:
    * One `python` task which creates some logs and stores them into several log tables within a `test.db` SQLite database. This database is created in your project's folder root.
    * Several `autosql` tasks which create data models including tables and views based on those logs.

You can open `test.db` and see the tables and views created by `sayn run`. You can use [DB Browser for SQLite](https://sqlitebrowser.org/dl/){target="\_blank"} in order to view the content of the database. As you can observe, `sayn run` created a small ETL process which models battles from various tournaments.

That's it, you made your first SAYN run! We will now explain what happens in the background.

## Project Overview

The `test_sayn` folder has the following structure:

```
  test_sayn    
    compile/ # only appears after first run     
    dags/
        base.yaml
    logs/ # only appears after first run
        sayn.log
    python/
        __init__.py
        load_data.py
        utils/
          __init__.py
          log_creator.py
    sql/
        dim_arenas.sql
        dim_fighters.sql
        dim_tournaments.sql
        f_battles.sql
        f_fighter_results.sql
        f_rankings.sql
    .gitignore
    project.yaml
    readme.md
    settings.yaml
```

Please see below the role of each component:

* `project.yaml`: defines the core components of the SAYN project. It is **shared across all collaborators**.
* `settings.yaml`: defines the individual user's settings. It is **unique for each collaborator and should never be pushed to git** as it contains credentials.
* `dags`: folder where DAG files are stored. SAYN tasks are defined in those files.
* `python`: folder where `python` tasks are stored.
* `sql`: folder where `sql` and `autosql` tasks are stored.
* `logs`: folder where SAYN logs are written.
* `compile`: folder where SQL queries are compiled before execution.

## Implementing Your Project

We will now go through the example project and explain the process of building a SAYN project. You can use the `test_sayn` folder you created to follow along, all the code is already written there.

### Step 1: Define the SAYN project with `project.yaml`

Add the `project.yaml` file at the root level of your directory. Here is the file from the example:

**`project.yaml`**
``` yaml
required_credentials:
  - warehouse

default_db: warehouse

dags:
  - base
```

The following is defined:

* `required_credentials`: the required credentials to run the project. Credential details are defined in the `settings.yaml` file.
* `default_db`: the database used at run time.
* `dags`: the DAGs of the project (this example has only one `dag` which can be found at `dags/base.yaml`). Those DAGs contain the tasks.

### Step 2: Define your individual settings with `settings.yaml`

Add the `settings.yaml` file at the root level of your directory. Here is the file from the example:

**`settings.yaml`**

``` yaml
default_profile: dev

profiles:
  dev:
    credentials:
      warehouse: test_db
  prod:
    credentials:
      warehouse: prod_db

credentials:
  test_db:
    type: sqlite
    database: test.db
  prod_db:
    type: sqlite
    database: prod.db
```

The following is defined:

* `default_profile`: the profile used by default at execution time.
* `profiles`: the list of available profiles to the user. Here we include the credential details for a `dev` and a `prod` profile.
* `credentials`: the list of credentials for the user.

### Step 3: Define your DAG(s)

In SAYN, DAGs are defined in `yaml` files within the `dags` folder. As seen before, those `dags` are imported in the `project.yaml` file in order to be executed. When importing the DAGs in the `project.yaml` file, you should write the name without the `.yaml` extension.

Our project contains only one DAG: `base.yaml`. Below is the file:

**`base.yaml`**
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
      tmp_schema: main
      schema: main
      table: dim_tournaments
    parents:
      - load_data

  dim_arenas:
    type: autosql
    file_name: dim_arenas.sql
    materialisation: table
    destination:
      tmp_schema: main
      schema: main
      table: dim_arenas
    parents:
      - load_data

  dim_fighters:
    type: autosql
    file_name: dim_fighters.sql
    materialisation: table
    destination:
      tmp_schema: main
      schema: main
      table: dim_fighters
    parents:
      - load_data

  f_battles:
    type: autosql
    file_name: f_battles.sql
    materialisation: table
    destination:
      tmp_schema: main
      schema: main
      table: f_battles
    parents:
      - load_data
      - dim_tournaments
      - dim_arenas
      - dim_fighters

  f_fighter_results:
    type: autosql
    file_name: f_fighter_results.sql
    materialisation: table
    destination:
      tmp_schema: main
      schema: main
      table: f_fighter_results
    parents:
      - f_battles

  f_rankings:
    type: autosql
    file_name: f_rankings.sql
    materialisation: view
    destination:
      tmp_schema: main
      schema: main
      table: f_rankings
    parents:
      - f_fighter_results
```

The following is defined:

* `tasks`: the tasks of the DAG.

Each task is defined by a `type` and various properties respective to its `type`. In our example, we use two task types:

* `python`: lets you run a Python process. The `load_data.py` is our only `python` task. It creates some synthetic logs and loads them to our database. For more information about how to build `python` tasks, visit the [Python section](../tasks/python.md)
* `autosql`: lets you write a `SELECT` statement and SAYN then creates the table or view automatically for you. Our example has multiple `autosql` tasks which create models based on the logs. For more information about setting up `autosql` tasks, visit the [autosql section](../tasks/autosql.md).

## Running Your Project

You can now run your SAYN project with the following commands:

* `sayn run`: runs the whole project
* `sayn run -p [profile_name]`: runs the whole project with the specific profile. In our case using the profile `prod` will create a `prod.db` SQLite database and process all data there.
* `sayn run -t [task_name]`: runs the specific task

More options are available to run specific components of your SAYN project. All details can be found in the [Commands](../commands.md) section.

## What Next?

You now know the basics of SAYN, congratulations! You can continue learning by going through the [Tutorial: Part 2](tutorial_part2.md) which shows you several tricks to make your SAYN project more dynamic and efficient.

Otherwise, you can go through the rest of the documentation as SAYN has much more to offer!

Enjoy SAYN :)

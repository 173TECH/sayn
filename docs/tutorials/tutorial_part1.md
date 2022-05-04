# Tutorial Part 1: Getting Started With SAYN

This tutorial navigates you through your first SAYN run and explains the core components of a SAYN project. It uses the example project created by `sayn init`. It assumes SAYN is setup as described in the
[installation section](../installation.md).

## Your First SAYN Run

To get started, open a terminal, activate your virtual environment (`source sayn_venv/bin/activate`) and run the following:

!!! example "getting started commands"
    ```bash
    sayn init sayn_tutorial
    cd sayn_tutorial
    sayn run
    ```

This will create a new project with the contents of this tutorial and execute it.

![`sayn run` execution](sayn_run1.gif)

You have made your first SAYN run! This executed several tasks that created SQL data models (several tables and one view) in the SQLite database `dev.db`. You can use
[DB Browser for SQLite](https://sqlitebrowser.org/dl/){target="\_blank"} in order to view the content of the database. These data models model battles from various tournaments and are similar to transformation processes you would run in-warehouse for analytics purposes.

Now that you have made your first SAYN run, let's cover what happens in the background.

## Project Overview

The `sayn_tutorial` folder has the following structure:

```
tutorial
├── project.yaml
├── settings.yaml
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
├── logs
├── .gitignore
├── readme.md
└── requirements.txt
```

The main files are:

* `project.yaml`: defines the SAYN project and the task groups. It is **shared across all collaborators**.
* `settings.yaml`: defines the individual user's settings. It is **unique for each collaborator and should never be pushed to git** as it contains credentials.
* `python`: folder where scripts for `python` tasks are stored.
* `sql`: folder where SQL files for `sql` and `autosql` tasks are stored.
* `logs`: folder where SAYN logs are written.
* `compile`: folder where compiled SQL queries before execution.

## Setting Up Your Project

Now let's see how the tutorial project would be created from scratch.

### Step 1: Define The Project In `project.yaml`

The `project.yaml` file is at the root level of your directory and contains:

!!! example "project.yaml"
    ```yaml
    required_credentials:
      - warehouse

    default_db: warehouse

    groups:
      models:
        type: autosql
        file_name: "*.sql"
        materialisation: table
        destination:
          table: "{{ task.name }}"

      logs:
        type: python
        module: load_data
    ```

The following is defined:

* `required_credentials`: the list of credentials used by the project. In this case we have a single credential called `warehouse`. The connection details will be defined in `settings.yaml`.
* `default_db`: the database used by sql and autosql tasks. Since we only have 1 credential, this field could be skipped.
* `groups`: these define the core task groups of the project. The project only has one task group which defines the `models` task group. More details on what groups do in the next section of the tutorial.

### Step 2: Define Your Individual Settings

Your individual settings are defined by the `settings.yaml` file which is stored at the root level of your directory. It contains:

!!! example "settings.yaml"
    ```yaml
    profiles:
      dev:
        credentials:
          warehouse: dev_db

    default_profile: dev

    credentials:
      dev_db:
        type: sqlite
        database: dev.db
    ```

The following is defined:

* `profiles`: the definion of profiles for the project. A profile defines the connection between credentials in the `project.yaml` file and your own credentials.
* `default_profile`: the profile used by default at execution time. This can be overriden if necessary via `-p` flag of the run command.
* `credentials`: here we define the credentials necessary to run the project. We simply define our `warehouse` credential which is currently a SQLite database.

## What Next?

You now know the core components and have made your first SAYN run, congratulations! In the [next section of the tutorial](tutorial_part2.md), we go through how to use SAYN for data modelling.

Enjoy SAYN :)

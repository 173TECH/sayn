# Tutorial

## Your first SAYN project

Make sure you have followed the steps from the [Getting Started](getting_started.md) section in order to install the SAYN Python package and create your first project folder. The project you installed with `sayn init` contains and example SAYN project. We will use this project through this tutorial to get you going with SAYN and explain core concepts.

If you have not done it yet, join our [public Slack channel](link_to_be_added) in order to get help with SAYN whenever needed (the developers and maintainers of SAYN are in this channel).

## Project structure overview

A SAYN project is composed of two key files:

- `models.yaml`: this is the backbone of a SAYN project where tasks and their parentage are defined. This is what SAYN will use to create and run the DAG. This file is shared across all users of the SAYN project.
- `settings.yaml`: this file is used for individual settings to run the SAYN project. This file is unique to each SAYN user on the project and is automatically ignored by git **(it should never be pushed to git as it contains credentials for databases and APIs used by the SAYN project)**. It enables SAYN user to set profiles for testing and overwrite the default project parameters.

Please see below example files for the above:

**`models.yaml`**
``` yaml
sayn_project_name: sayn_chinook

default_db: warehouse

required_credentials:
  - warehouse

parameters:
  table_prefix: ''
  schema_logs: analytics_logs
  schema_staging: analytics_staging
  schema_models: analytics_models

groups:
  modelling:
    type: autosql
    materialisation: table
    to:
      staging_schema: '{{schema_staging}}'
      schema: '{{schema_models}}'
      table: '{{table_prefix}}{{task.name}}'

tasks:
  track_details_sql:
    file_name: track_details_sql.sql
    type: sql

  track_details_autosql:
    file_name: track_details_autosql.sql
    type: autosql
    materialisation: table
    to:
      staging_schema: '{{schema_staging}}'
      schema: '{{schema_models}}'
      table: '{{table_prefix}}{{task.name}}'

  tracks_per_album:
    file_name: tracks_per_album.sql
    group: modelling

  tracks_per_album_ordered:
    file_name: tracks_per_album_ordered.sql
    group: modelling
    materialisation: view
    parents:
      - tracks_per_album

  print_top_10_albums_by_tracks:
    module: print_top_10_albums_by_tracks
    type: python
    class: TopAlbumsPrinter
    parents:
      - tracks_per_album_ordered
```

The `models.yaml` has the following parameters defined, which are all mandatory:

- `sayn_project_name`: the name of your SAYN project.
- `default_db`: this is the database that will be used by tasks writing to the analytics warehouse (e.g. `sql`, `autosql` tasks). The `default_db` that is set should be part of the `required_credentials`.
- `required_credentials`: the list of database and API credentials which are required to run the SAYN project. The credential details are then specified in `settings.yaml`.
- `parameters`: parameters are used to customise tasks. SQL queries are effectively Jinja templates so those parameters can be used in queries. They can also be access in Python tasks via the task object attributes. `parameters` can be overridden with `settings.yaml` which means that user can set specific `parameters` for testing or separate development environments.
- `groups`: groups enable to define some attributes which will be used by all tasks referring to the group.
- `tasks`: the list of tasks and their definitions. Those will be used by SAYN to create and run the DAG.

*Note: SAYN has a range of tasks that can be used to implement and automate data processes. This tutorial will only cover the `sql`, `autosql` and `python` tasks but more task types are available. For more information about tasks please see the [Tasks](tasks.md) section of the documentation.*

**`settings.yaml`**

``` yaml
default_profile: test

profiles:
  test:
    credentials:
      warehouse: chinook

    parameters:
      table_prefix: tu_
      schema_logs: analytics_logs
      schema_staging: analytics_adhoc
      schema_models: analytics_adhoc

credentials:
  chinook:
    type: sqlite
    database: chinook.db
```

The `settings.yaml` has the following parameters defined, which are all mandatory:

- `default_profile`: the profile that will be used by default when running SAYN. The profile specified needs to be defined in the `profiles`.
- `profiles`: the list of profiles with credential and parameter details. Each credential specified should be defined in the `credentials`.
- `credentials`: the list of credentials for the SAYN project.

The SAYN project also has the following folders:

- `logs`: the folder in which SAYN logs are stored by default.
- `sql`: the folder in which SQL task queries should be stored
- `python`: the folder in which Python task modules should be stored

You should now have a good understanding of the core files and folders in the SAYN project. We will now go over the SAYN example project installed by `sayn init` and do our first SAYN run.

## Implementing your project

We will now go through the example project and process our first run with SAYN (the project uses a SQLite database). Although the example project has everything defined for you and should run already, we will go through the steps as if we were implementing the project.

### Step 1: Define your individual settings with `settings.yaml`

We will define our individual settings for the SAYN project. Those will go into the `settings.yaml` file which is unique to each user across the project.

#### Step 1.1: Set the profile\(s\)

First we will define our profiles. Our profiles define credentials (used to connect to databases and APIs) and parameters (used to customise tasks) for each profile. We define the profiles as follows:

```yaml
profiles:
  test:
    credentials:
      warehouse: chinook

    parameters:
      table_prefix: tu_
      schema_logs: main
      schema_staging: main
      schema_models: main
```

**Tip:** Although not done in the example, it is usually a good idea to define a `test` (for development) and a `prod` (for production) profile and use the `test` profile as a default. This enables you to switch easily between profile whenever necessary.

*Note: the schemas are all defined as main. This is because SQLite does not have schemas and we refer to the main database.*

#### Step 1.2: Set the default profile

Now that we have defined profiles, we will set the default profile we want to use when running SAYN. We will default to using the `test` profile which is the only profile defined in the example project. At the top of `settings.yaml`, we add the following:

```yaml
default_profile: test
```

#### Step 1.3: Set the credentials

The last part is of `settings.yaml` is setting the `credentials` which will be used by the profiles. We add the following at the end of the file.

```yaml
credentials:
  chinook:
    type: sqlite
    database: chinook.db
```

*Note: different databases require different connection parameters. For specific detail regarding connecting to different databases please see the [Database](database.md) section of the documentation.*

This is it for `settings.yaml`, the SAYN project will now know which profile, parameters and credentials to use at run time.

### Step 2: Define the project tasks with `models.yaml`

We will now define the backbone of the SAYN project. This will be done with the `models.yaml` file which is shared by all users across the project.

#### Step 2.1: Set the project name

At the top of the file, we will add the following:

```yaml
sayn_project_name: sayn_chinook
```

*Note: it is good practice to give your project a name that is meaningful, such as \[company\]_sayn_etl.*

#### Step 2.2: Set the default database

We will now set the default database that will be used by SAYN when running. This database will be used when running SQL tasks and can be accessed in Python tasks via the task's attributes (more on this below). Add the following to `models.yaml`.

```yaml
default_db: warehouse
```

#### Step 2.3: Set the required credentials

Those are the credentials which are required by the SAYN project to run. Any credential listed here should be defined in the `credentials` section of the `settings.yaml` file. Our example project only requires the SQLite database credentials, so we add the following to our `models.yaml` file:

```yaml
required_credentials:
  - warehouse
```

#### Step 2.4: Set the project's default parameters

As mentioned before, the parameters are used to customise SAYN tasks. Those parameters will be used by default when running SAYN, unless the parameters are overridden in the profile used at run time. Add the following to `models.yaml`:

```yaml
parameters:
  table_prefix: ''
  schema_logs: main #this is specific to SQLite (this should be the database name which is main)
  schema_staging: main #this is specific to SQLite (this should be the database name which is main)
  schema_models: main #this is specific to SQLite (this should be the database name which is main)
```

We have defined a `table_prefix` which is useful for development, as each analyst can use their initials to prefix tables and views. This prevents conflicts in the event users are doing some testing that requires using the same tables and views.
We also have defined `schema_logs`, `schema_staging` and `schema_models`. Although this is not really useful in our example as SQLite does not support schemas, this becomes really useful on databases used for analytics (i.e. Redshift, Snowflake, etc.) as this enables test profiles to use test schemas and production profiles to use the production schemas.

#### Step 2.5: Set the group\(s\)

Groups can be used in order to define attributes which can be used by multiple tasks. Tasks, which we will define in the next step, can refer to groups. When doing so, the task will inherit the attributes of the group it refers to. We will define one group, for modelling tasks. Add the following  to `models.yaml`:

```yaml
groups:
  modelling:
    type: autosql
    materialisation: table
    to:
      staging_schema: '{{schema_staging}}'
      schema: '{{schema_models}}'
      table: '{{table_prefix}}{{task.name}}'
```

The attributes defined in the `modelling` group will become clearer when we cover tasks in the next section. In this instance, the `modelling` group mentions that all tasks of this group are of the type `autosql` (defined in the next section) and will all use the other defined parameters.

#### Step 2.6: Set the tasks

We will now define the tasks that will compose the SAYN project. Tasks all have a type (we will only cover the `sql`, `autosql` and `python` types in this tutorial). Tasks then define specific parameters based on the task type and can specify parents to create relationships between tasks. Let's create our tasks, add the following code to the end of `models.yaml`.

```yaml
tasks:
  track_details_sql:
    file_name: track_details_sql.sql
    type: sql

  track_details_autosql:
    file_name: track_details_autosql.sql
    type: autosql
    materialisation: table
    to:
      staging_schema: '{{schema_staging}}'
      schema: '{{schema_models}}'
      table: '{{table_prefix}}{{task.name}}'

  tracks_per_album:
    file_name: tracks_per_album.sql
    group: modelling

  tracks_per_album_ordered:
    file_name: tracks_per_album_ordered.sql
    group: modelling
    materialisation: view
    parents:
      - tracks_per_album

  print_top_10_albums_by_tracks:
    module: print_top_10_albums_by_tracks
    type: python
    class: TopAlbumsPrinter
    parents:
      - tracks_per_album_ordered
```

Let's cover each task one by one.

----

The first task, `track_details_sql` is a `sql` task:

```yaml
track_details_sql:
  file_name: track_details_sql.sql
  type: sql
```

 `sql` tasks enable you to run a SQL query. The query should be saved at the location indicated by the `file_name` attribute within the `sql` folder.

Here is the SQL query referred by the `track_details_sql` task:

**`sql/track_details_sql.sql`**
```sql
/*
In this sql query, you can see the usage of parameters.
This is based on Jinja templating.
Parameters are passed from the profile used at run time (profiles are defined in the settings)
After a query is compiled, they will appear in the compile folder
*/

DROP TABLE IF EXISTS {{schema_models}}.{{table_prefix}}track_details_sql
;

CREATE TABLE {{schema_models}}.{{table_prefix}}track_details_sql AS

SELECT t.trackid
     , t.name
     , al.title album_name
     , ar.name artist_name

FROM {{schema_logs}}.tracks t

INNER JOIN {{schema_logs}}.albums al
  ON t.albumid = al.albumid

INNER JOIN {{schema_logs}}.artists ar
  ON al.artistid = ar.artistid
;
```

You can see here that this SQL query is referring to our parameters. SAYN uses Jinja templating which enables you to use your parameters when writing queries. For example, if using the `test` profile, the `table_prefix` `tu_` would be used. Otherwise the empty prefix `''` would be used as defined in the default parameters in `models.yaml`. You could obviously hardcode everything, but as we mentioned earlier, this is not a good idea as you ideally want to separate development and production environments.

----

The second task is `track_details_autosql` which is an `autosql` task:

```yaml
track_details_autosql:
  file_name: track_details_autosql.sql
  type: autosql
  materialisation: table
  to:
    staging_schema: '{{schema_staging}}'
    schema: '{{schema_models}}'
    table: '{{table_prefix}}{{task.name}}'
```

`autosql` tasks enable you to simply write a `SELECT` statement and SAYN will automatically create the table or view for you. See the [Tasks](tasks.md) section of the documentation for more information on this task type. `autosql` only requires you to define the `materialisation` type and the process parameters: `staging_schema` (for the temporary table created in the process), `schema` (the destination schema where the object will be created), `table_name` (the name of the object created). In addition, you can see the attributes of the `track_details_autosql` task refer to our `parameters` and also the `task.name` which is  `track_details_autosql`. For example, if you had a `prod` profile with `schema_models` `analytics_models` and a `test` profile with `schema_models` `analytics_adhoc`, then this task would create the table in the `analytics_models` schema when using the `prod` profile and it would write it  in the `analytics_adhoc` schema  when using the `test` profile.

Here is the SQL query referred by the `track_details_autosql` task. As you can observe, this query now does not have the `DROP` and `CREATE` statements. It only has the `SELECT` statement, everything is handled for you by SAYN.

**`sql/track_details_autosql.sql`**
```sql
/*
In this sql query, you can see the usage of parameters.
This is based on Jinja templating.
Parameters are passed from the profile used at run time (profiles are defined in the settings)
After a query is compiled, they will appear in the compile folder
*/

SELECT t.trackid
     , t.name
     , al.title album_name
     , ar.name artist_name

FROM {{schema_logs}}.tracks t

INNER JOIN {{schema_logs}}.albums al
  ON t.albumid = al.albumid

INNER JOIN {{schema_logs}}.artists ar
  ON al.artistid = ar.artistid
```

-----

The third task is `tracks_per_album` which is using a `group` for definition:

```yaml
tracks_per_album:
  file_name: tracks_per_album.sql
  group: modelling
```

Because this task refers to the `modelling` group, it inherits all the settings from this group, which are:

```yaml
groups:
  modelling:
    type: autosql
    materialisation: table
    to:
      staging_schema: '{{schema_staging}}'
      schema: '{{schema_models}}'
      table: '{{table_prefix}}{{task.name}}'
```

Therefore, the `tracks_per_album` task is an `autosql` task, materialises as a table and has the other attributes of the `modelling` group. When running this task with the `test` profile, it will therefore use `main` as the `staging_schema` and `schema`, and the table will be name `tu_tracks_per_album`.

Here is the SQL query referred by the `track_per_album` task.

**`sql/tracks_per_album.sql`**
```sql
/*
In this sql query, you can see the usage of parameters.
This is based on Jinja templating.
Parameters are passed from the profile used at run time (profiles are defined in the settings)
After a query is compiled, they will appear in the compile folder
*/

SELECT al.title album_name
     , COUNT(DISTINCT t.trackid) n_tracks

FROM {{schema_logs}}.tracks t

INNER JOIN {{schema_logs}}.albums al
  ON t.albumid = al.albumid

GROUP BY 1
```

----

The fourth task is `tracks_per_album_ordered`:

```yaml
tracks_per_album_ordered:
  file_name: tracks_per_album_ordered.sql
  group: modelling
  materialisation: view
  parents:
    - tracks_per_album
```

As you can see, it inherits from the `modelling` group. However, it overrides the `materialisation` to be a view. Finally it sets `parents` to be the `tracks_per_album` task. This means this task will always be executed after the `tracks_per_album` task is successfully finished. If the `tracks_per_album` fails, the `tracks_per_album_ordered` task will be skipped.

*Note: a task can have as many parents as desired.*

Here is the SQL query referred by the `track_per_album_ordered` task.

**`sql/tracks_per_album_ordered.sql`**
```sql
/*
In this sql query, you can see the usage of parameters.
This is based on Jinja templating.
Parameters are passed from the profile used at run time (profiles are defined in the settings)
After a query is compiled, they will appear in the compile folder
*/

SELECT tpa.*

FROM {{schema_models}}.{{table_prefix}}tracks_per_album tpa --here we prepend the table name with {{table_prefix}} which enables to separate when testing. If ran from prod, there is no prefix. If a test user has prefix specified, then the prefix will be added.

ORDER BY 2 DESC
```

----

The fifth and final task is `print_top_10_albums_by_tracks` and is a `python` task:

```yaml
print_top_10_albums_by_tracks:
  module: print_top_10_albums_by_tracks
  type: python
  class: TopAlbumsPrinter
  parents:
    - tracks_per_album_ordered
```

A `python` task enables you to use Python for your task. This means you could do anything, from extracting data from an API to running a data science model. `python` tasks require a `module` (this is the name of python file in the `python` folder. SAYN automatically looks there fore Python tasks) and a `class` (this should be the name of the class in the Python module.)

Here is the Python codee referred by the `print_top_10_albums_by_tracks` task.

**`python/print_top_10_albums_by_tracks.py`**
```python
#IMPORTANT: for python tasks to be able to execute, you neeed to have an __init__.py file into the python folder so it is treated as a package
#here we define a python task
#python tasks inherit from the sayn PythonTask
from sayn import PythonTask

#a python task needs to implement two functions
#setup() wich operates necessary setup and returns self.ready() to indicate the task is ready to be ran
#run() which executes the task and returns self.finished() to indicate the task has finished successfully
class TopAlbumsPrinter(PythonTask):
    #here we are not doing anything for setup, just displaying the usage of self.failed()
    #in order to inform sayn that the task has failed, you would return self.failed()
    #note that self.failed() can also be used for run()
    #please note that setup() needs to follow the method's signature. Therefore it needs to be set as setup(self).
    def setup(self):
        #code doing setup
        err = False
        if err:
            return self.failed()
        else:
            return self.ready()

    #here we define the code that will be executed at run time
    #please note that run() needs to follow the method's signature. Therefore it needs to be set as run(self).
    def run(self):
        #we can access the project parameters via the sayn_config attribute
        sayn_params = self.sayn_config.parameters
        #we use the config parameters to make the query dynamic
        #the query will therefore use parameters of the profile used at run time
        q = '''
            SELECT tpao.*

            FROM {schema_models}.{table_prefix}tracks_per_album_ordered tpao

            LIMIT 10
            ;
            '''.format(
                schema_models=sayn_params['schema_models'],
                table_prefix=sayn_params['table_prefix']
            )

        #the python task has the project's default_db connection object as an attribute.
        #this attribute has an number of method including select() to run a query and return results.
        #please see the documentation for more details on the API
        r = self.default_db.select(q)

        print('Printing top 10 albums by number of tracks:')
        for i in range(10):
            print('#{rank}: {album}, {n} tracks.'.format(rank=i+1, album=r[i]['album_name'], n=r[i]['n_tracks']))

        return self.finished()
```

Please note that for the Python tasks to run properly, **you need to have an `__init__.py` file into the `python` folder so it is considered as a package**.

The `TopAlbumsPrinter` Python class implements two methods: `setup` and `run`. The `setup` method will be run when the DAG sets up all the tasks, and the `run` method will be run at execution time. Both methods need to have the signatures of the SAYN `PythonTask` class. As a result, you should only pass `self` as a parameter to those methods.

A few things to note regarding `python` tasks:

- If you want to have the task fail (e.g. to control for errors), return `self.failed()` in the methods.
- As you can see in the `run` method, we are accessing a few useful attributes of the task.
- `self.sayn_config.parameters` contains the parameters used.
- `self.default_db` contains the default database.
- `self.debault_db.select()` effectively runs a select query on the default database.

For more details on the `python` tasks, please refer to the [Tasks](task.md) documentation.

This it, we now have set all our tasks and our project is ready to be ran :)

## Running your project

Now that the `settings.yaml` have our individual settings and that `models.yaml` have the tasks defined, we can run the sayn project! Run the following command in order to run the whole project (make sure you are at the root level of your SAYN project directory): `sayn run`.

`sayn run` will run the whole project using the profile set in `default_profile`. A SAYN run is composed of setting up the DAG (when all tasks are setup) and running the DAG (when the DAG is executed). You should see the detail of the process being logged to your command line (those logs are also saved in the log folder).

**Tip:** after running `sayn run`, you will also see a `compile` folder appear. This is where all SQL queries will be compiled by SAYN so you can check there the output after the templates are filled.

The `sayn run` command has a few option parameters:
- `-t` in order to run a specific task. For example `sayn run -t track_details_sql` would only run the `track_details_sql` task.
- `-d` can be used in order to print debug logs to the command line.

**Tip:** you can run a task and all its parents or children by prefixing or suffixing the task name after the `-t` flag with `+`. For example, running `sayn run -t +print_top_10_albums_by_tracks` would run the `print_top_10_albums_by_tracks` and all its parents in their logical order.

## What Next?

This is it, you have now completed the SAYN tutorial, congratulations! You now know how to implement and run a SAYN project and are able to use SAYN to increase your analytics workflow efficiencies.

You should now have a look into the further details of the documentation, as SAYN has much more to offer. In specific, you can have a look at the following sections:

- S1
- S2

Enjoy SAYN!

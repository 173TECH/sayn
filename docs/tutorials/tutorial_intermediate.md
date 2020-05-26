# Tutorial: Intermediate Usage

## What We Will Cover

This tutorial builds upon the [Tutorial: Basic Usage](tutorial_basic.md) and introduces you to SAYN concepts which will enable you to make your projects more dynamic and efficient. This tutorial will be close to how you might setup SAYN on production.

If you need any help or want to ask a question, please reach out to the team at <sayn@173tech.com>.

## Implementing Intermediate Concepts

The [Tutorial: Basic Usage](tutorial_basic.md) got you to implement a first ETL process with SAYN. We will now look into two core SAYN features which will enable you to build efficient and dynamic projects as you scale: `parameters` and `presets`.

For this tutorial, we will now use a PostgreSQL database. You should clone this [GitHub repository](ADD LINK) in order to get the code and follow this tutorial - everything is already written for you. The synthetic data is the same than for the [Tutorial: Basic Usage](tutorial_basic.md).

### Step 0: Setup And Quick Run

#### Setup Credentials

In order to run this project, you need to change the `credentials` in `settings.yaml`. As mentioned, this project is setup to work using a PostgreSQL database. If you want to use another database, you will need to change the SQL code syntax to the relevant database.

#### Create Database Schemas

We want to be able to differentiate between our development and production environments. Our PostgreSQL database will have four schemas:

* analytics_adhoc: used for testing
* analytics_logs: used to store logs
* analytics_staging: used for staging processes
* analytics_models: used to create our production data models

#### Run

Run the command `sayn run` in order to do a first run. This should create a set of tables and views in your `analytics_adhoc` schema.

### Step 1: Setting Up `parameters`

In order to be able to test in our development environment and then release on production, we will use `parameters`. Those will enable us to make our tasks' code dynamic depending on the profile used for execution.

First, add `paramaters` to `project.yaml`. Those `parameters` will contain the default values for the SAYN project:

**`project.yaml`**
``` yaml
default_db: warehouse

required_credentials:
  - warehouse

dags:
  - base

parameters:
  user_prefix: '' #no prefix for prod
  schema_logs: analytics_logs
  schema_staging: analytics_staging
  schema_models: analytics_models
```

Then, add the `parameters` to your profiles in `settings.yaml`. We will use two profiles:

* `dev`: for development
* `prod`: for production

Please note that, although we use the same credentials for both profiles, you should use different ones on production setups.

**`settings.yaml`**
``` yaml
default_profile: dev

profiles:
  dev:
    credentials:
      warehouse: tutorial_db
    parameters:
      user_prefix: sg_
      schema_logs: analytics_adhoc
      schema_staging: analytics_adhoc
      schema_models: analytics_adhoc
  prod:
    credentials:
      warehouse: tutorial_db
    parameters:
      user_prefix: '' #no prefix for prod
      schema_logs: analytics_logs
      schema_staging: analytics_staging
      schema_models: analytics_models

credentials:
  tutorial_db:
    type: postgresql
    connect_args:
      host: your-host.com
      port: 5432
      user: username
      password: password
      dbname: database
```

Please make sure that you change the credentials to your PostgreSQL database configuration.

*Note: if your password starts with special characters (e.g. @), make sure you wrap it within single quotes - for example '@@mypassword'. Otherwise, the YAML will not work.*

You can now run SAYN using the two above profiles. Running `sayn run -p prod` will create your tables and views in the relevant production schemas set by your `prod` profile whilst `sayn run` will default to the `dev` profile.

### Step 2: Making Tasks Dynamic With `parameters`

Now that our parameters are setup, we can easily make the tasks dynamic and control the source and destination of processes. For example, we are using `parameters` to:

* load our logs in the schema `analytics_adhoc` when testing, `analytics_logs` on production.
* create models in the schema `analytics_adhoc` when testing, `analytics_models` on production.
* always prefix the created models with the user's initials when testing

This is done as follows in our tasks' code:

**`load_data.py`**
```python
# ...

def run(self):

      #we want to load the logs to the relevant schema depending whether we are testing or running on production
      user_prefix = self.sayn_config.parameters['user_prefix']
      schema_logs = self.sayn_config.parameters['schema_logs']
      prefix_logs = schema_logs + '.' + user_prefix

      # this prefix_logs variable is then used to customise the log load destination
      # ...
```

**`dim_arenas.sql`**
```sql
SELECT l.payload->>'arena_id' arena_id
     , l.payload->>'arena_name' arena_name

--we make the schema and user_prefix dynamic
--please note that {{prefix_logs}} is defined in the modelling preset, see next section
FROM {{prefix_logs}}logs l

WHERE event_type = 'arenaCreation'
```

### Step 3: Using `presets` To Standardise Task Definitions

Because most of our tasks have a similar configuration, we can significantly reduce the YAML task definitions using `presets`. `presets` enable to create standardised tasks which can be used to define other tasks by setting their preset attribute. We define a `modelling` preset and use it in our DAG `base.yaml`.

**`dags/base.yaml`**
```yaml
presets:
  modelling:
    type: autosql
    file_name: '{{task.name}}.sql'
    materialisation: table
    destination:
      tmp_schema: '{{schema_staging}}'
      schema: '{{schema_models}}'
      table: '{{user_prefix}}{{task.name}}'
    parameters:
      prefix_logs: '{{schema_logs}}.{{user_prefix}}'
      prefix_models: '{{schema_models}}.{{user_prefix}}'

tasks:
  load_data:
    type: python
    class: load_data.LoadData

  #this task sets modelling as its preset attribute
  #therefore it inherits all the attributes from the modelling preset
  dim_tournaments:
    preset: modelling
    parents:
      - load_data

  dim_arenas:
    preset: modelling
    parents:
      - load_data

  dim_fighters:
    preset: modelling
    parents:
      - load_data

  f_battles:
    preset: modelling
    parents:
      - load_data
      - dim_tournaments
      - dim_arenas
      - dim_fighters

  f_fighter_results:
    preset: modelling
    parents:
      - f_battles

  #for that task, we overwrite the modelling preset materialisation attribute as we want this model to be a view
  f_rankings:
    preset: modelling
    materialisation: view
    parents:
      - f_fighter_results
```

## What Next?

This is it, you should now have a good understanding of the core ways of using SAYN. In order to learn more about specific features, have a look at the specific sections of the documentation.

Enjoy SAYN :)

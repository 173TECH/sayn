# Settings: `settings.yaml`

## Role

The `settings.yaml` file is used for individual settings to run the SAYN project. **This file is unique to each SAYN user** collaborating on the project and is automatically ignored by git **(it should never be pushed to git as it contains credentials for databases and APIs used by the SAYN project)**.

## Content

### Overview

Please see below and example of `settings.yaml` file:

**`settings.yaml`**

``` yaml
default_profile: dev

profiles:
  dev:
    credentials:
      warehouse: snowflake-songoku

    parameters:
      table_prefix: songoku_
      schema_logs: analytics_logs
      schema_staging: analytics_adhoc
      schema_models: analytics_adhoc

  prod:
    credentials:
      warehouse: snowflake-prod

    # no need for prod parameters as those are read from models.yaml

credentials:
  snowflake-songoku:
    type: snowflake
    connect_args:
      account: [snowflake-account]
      user: [user-name]
      password: '[password]'
      database: [database]
      schema: [schema]
      warehouse: [warehouse]
      role: [role]

  snowflake-prod:
    type: snowflake
    connect_args:
      account: [snowflake-account]
      user: [user-name]
      password: '[password]'
      database: [database]
      schema: [schema]
      warehouse: [warehouse]
      role: [role]
```

The `settings.yaml` file requires the following to be defined:

* `default_profile`: the profile used by default at execution time.
* `profiles`: the list of available profiles to the user. `credentials` and `parameters` are defined for each profile. Those `parameters` overwrite the `parameters` set in `project.yaml`.
* `credentials`: the list of credentials for the user.

As can be observed, this file enables the user to use two different profiles whenever desired: `dev` and `prod`. It is usually good practice to separate your environments in order to ensure that testing is never done directly on production.

### About Credentials

The `credentials` section of the `settings.yaml` file is used to store both databases and API credentials.

#### Databases

SAYN supports various databases. In order to create a database credential, define the `type` as one of the supported databases (see the [Database](../databases/overview.md) section for more details) and the connection parameters relevant to the database type.

#### APIs

In order to define API credentials, use the `type: api` and pass the API connection parameters. Please see an example below:

```yaml
# ...

credentials:
  # ...

  credential_name:
    type: api
    api_key: 'api_key'
```

Those API credentials can then be accessed in `python` tasks through the Task object.

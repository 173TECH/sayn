# Settings: `settings.yaml`

## Role

The `settings.yaml` file is used for individual settings to run the SAYN project. This file is unique to each SAYN user on the project and is automatically ignored by git **(it should never be pushed to git as it contains credentials for databases and APIs used by the SAYN project)**. It enables the SAYN user to set profiles for testing / prod and overwrite the default project parameters. The profile used can be controlled by the user when running SAYN.

## Content

### Overview

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
    account: [snowflake-account]
    user: [user-name]
    password: '[password]'
    database: [database]
    schema: [schema]
    warehouse: [warehouse]
    role: [role]

  snowflake-prod:
    type: snowflake
    account: [snowflake-account]
    user: [user-name]
    password: '[password]'
    database: [database]
    schema: [schema]
    warehouse: [warehouse]
    role: [role]
```

All parameters in `settings.yaml` are mandatory except `default_profile` which is mandatory only if two or more profiles are defined. Please see below details on the parameters:

- `default_profile`: the profile that will be used by default when running SAYN. The profile specified needs to be defined in the `profiles`.
- `profiles`: the list of profiles with detail about which credential and parameter they should use.
- `credentials`: the details of credentials for the SAYN project. Here, the user has details for the Snowflake warehouse connection.

In the case of this settings file:

- The `warehouse` credential for the `test` profile should be `snowflake-tu` (the individual test user's credentials) and it should be `snowflake-prod` for the `prod` profile.
- The `test` profile would use `analytics_adhoc` for the parameter `schema_models`. The `prod` profile would use `analytics_models`.

This enables to easily switch between development and production environments which is best practice. More details about this can be found in the [Parameters](parameters.md) section.

### API Credentials

The `credentials` section of the `settings.yaml` file is used to store both databases and API credentials. Database credentials are covered above. For API credentials, those can be added using the type `api` and any parameter required for the API connection. Please see an example below:

```yaml
# ...

credentials:
  # ...

  credential_name:
    type: api
    api_key: 'api_key'
```

Those API credentials can then be accessed in `python` tasks through the `task` object.

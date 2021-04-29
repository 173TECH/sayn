# Settings: `settings.yaml`

The `settings.yaml` defines local configuration like credentials. **This file is unique to each SAYN user**
collaborating on the project and is automatically ignored by git.

!!! warning
    `settings.yaml` should never be pushed to git as it contains credentials for
    databases and APIs used by the SAYN project.

!!! example "settings.yaml"
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

        # no need for prod parameters as those are read from project.yaml

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

| Property | Description | Default |
| -------- | ----------- | -------- |
| profiles | A map of available profiles to the user. `credentials` and `parameters` are defined for each profile. Those `parameters` overwrite the `parameters` set in `project.yaml`. | Required |
| default_profile | The profile used by default at execution time. | Entry in `required_credentials` if only 1 defined |
| credentials | The list of credentials used in profiles to link `required_credentials` in `project.yaml`. | Required |

This file enables the user to use two different profiles whenever desired: `dev` and `prod`. It is
usually good practice to separate your environments in order to ensure that testing is never done directly
on production.

### Defining Credentials

Credentials includes both databases (eg: your warehouse) as well as custom secrets used by python tasks.
For a definition of a database connection see to the documentation for your
[database type](../databases/overview.md)

For custom credentials, use the `type: api` and include values required:

!!! example "settings.yaml"
    ```yaml
    credentials:
      credential_name:
        type: api
        api_key: 'api_key'
    ```

All credentials are accessible through `self.connections['credential_name']` where `credential_name` is the
name given in required_credentials. API credentials when accessed in python are defined as dictionary,
whereas database connections are `Database` objects.

### Using Environment Variables

Local settings can be set without the need of a `settings.yaml` file using environment variables instead.
With environment variables we don't need to set profiles, only credentials and project parameters are
defined. SAYN will interpret any environment variable names `SAYN_CREDENTIAL_name` or `SAYN_PARAMETER_name`.
The values when using environment variables are either basic types (ie: strings), json or yaml encoded.

Taking the `settings.yaml` example above for the dev profile, in environment variables:

!!! example ".env.sh"
    ```bash
    # JSON encoded credential
    export SAYN_CREDENTIAL_warehouse='{"type": "snowflake", "account": ...}'

    # YAML encoded credential
    export SAYN_CREDENTIAL_backend="
    type: postgresql
    host: host.address.com
    user: ...
    "

    # Project parameters as strings
    export SAYN_PARAMETER_table_prefix="songoku_"
    export SAYN_PARAMETER_schema_logs="analytics_logs"
    export SAYN_PARAMETER_schema_staging="analytics_adhoc"
    export SAYN_PARAMETER_schema_models="analytics_adhoc"

    # Project parameters allow complex types JSON or YAML encoded
    export SAYN_PARAMETER_dict_param="
    key1: value1
    key2: value2
    "
    ```

When environement variables are defined and a `settings.yaml` file exists, the settings from both will
be combined with the environment variables taking precedence.

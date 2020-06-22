# Redshift

## Connecting with regular users

SAYN will consider the following parameters to construct the sqlalchemy url:

- **host**
- **port**
- **user**
- **password**
- **dbname**

Other parameters specified will be passed to
[sqlalchemy.create_engine](https://docs.sqlalchemy.org/en/13/core/engines.html#sqlalchemy.create_engine)
when creating the engine.

!!! example "settings.yaml"
    ```yaml
    ...

    credentials:
      redshift-conn:
        type: postgresql
        host: [host]
        port: [port]
        user: [username]
        password: '[password]' #use quotes to avoid conflict with special characters
        dbname: [database_name]

    ...
    ```

Check the sqlalchemy [psycopg2](https://docs.sqlalchemy.org/en/13/dialects/postgresql.html#module-sqlalchemy.dialects.postgresql.psycopg2)
dialect for extra parameters.

## Connecting with IAM

You can connect to Redshift with SAYN through IAM users. In order to do so, add `cluster_id` to your credentials in `settings.yaml`. SAYN will use `boto3` to get a temporary password using AWS IAM.

!!! example "settings.yaml"
    ```yaml
    ...

    credentials:
      redshift-conn:
        type: postgresql
        host: [host]
        port: [port]
        user: [username]
        password: '[password]' #use quotes to avoid conflict with special characters
        dbname: [database_name]
        cluster_id: [redshift-cluster-id]

    ...
    ```

For this connection methodology to work:

* `boto3` needs to be installed in the project virtual environment `pip install boto3`.
* AWS credentials need to be
    [setup](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-welcome.html).
* The `user` and `dbname` still need to be specified (use the database user, not the `IAM:user`).
* `host` and `port` can be skipped and these values will be obtained using boto3's
    `redshift describe-clusters`.

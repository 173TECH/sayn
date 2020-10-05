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
        type: redshift
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
        type: redshift
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

## Redshift specific DDL

# Indexes

Redshift doesn't support index definitions, and so autosql and copy tasks will forbid its definition in the `ddl` entry in the task definition.

# Sorting

Table sorting can be specified under the `ddl` entry in the task definition

!!! example "dags/base.yaml"
    ```yaml
    ...

tasks:
  f_battles:
    type: autosql
    file_name: f_battles.sql
    materialisation: table
    destination:
      table: f_battles
    ddl:
      sorting:
        columns:
          - arena_name
          - fighter1_name
    ...
    ```

With the above example, the table `f_battles` will be sorted by `arena_name` and `fighter1_name` using a compound key (Redshift default). The type of sorting can be changed to interleaved.

!!! example "dags/base.yaml"
    ```yaml
    ...

tasks:
  f_battles:
    type: autosql
    file_name: f_battles.sql
    materialisation: table
    destination:
      table: f_battles
    ddl:
      sorting:
        type: interleaved
        columns:
          - arena_name
          - fighter1_name
    ...
    ```

For more information, read the latest docs about [SORTKEY](https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_NEW.html)

# Distribution

We can also specify the type of distribution: even, all or key based. If not specified, the Redshift default is even distribution.

!!! example "dags/base.yaml"
    ```yaml
    ...

tasks:
  f_battles:
    type: autosql
    file_name: f_battles.sql
    materialisation: table
    destination:
      table: f_battles
    ddl:
      distribution: all
    ...
    ```

If we want to distribute the table by a given column use the following:

!!! example "dags/base.yaml"
    ```yaml
    ...

tasks:
  f_battles:
    type: autosql
    file_name: f_battles.sql
    materialisation: table
    destination:
      table: f_battles
    ddl:
      distribution: key(tournament_name)
    ...
    ```

For more information, read the latest docs about [DISTKEY](https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_NEW.html)

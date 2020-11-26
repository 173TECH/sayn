# Redshift

The Redshift driver depends on [psycopg2](https://www.psycopg.org){target="\_blank"} and can be
installed with:

```bash
pip install "sayn[redshift]"
```

The [Redshift](https://aws.amazon.com/redshift/){target="\_blank"} connector looks for the following parameters:

Parameter  | Description                           | Default
---------  | ------------------------------------- | ---------------------------------------------
host       | Host name or public IP of the cluster | Required on standard user/password connection
port       | Connection port                       | 5439
user       | User name used to connect             | Required
password   | Password for that user                | Required on standard user/password connection
cluster_id | Cluster id as registered in AWS       |
dbname     | Database in use upon connection       | Required

For advanced configurations, SAYN will pass other parameters to `create_engine`, so check the
sqlalchemy [psycopg2](https://docs.sqlalchemy.org/en/13/dialects/postgresql.html#module-sqlalchemy.dialects.postgresql.psycopg2){target="\_blank"}
dialect for extra parameters.

## Connection types

SAYN supports 2 connection models for Redshift: standard user/password connection and IAM based.

### Standard user/password connection

If you have a user name and password for redshift use the first model and ensure host and password
are specified.

!!! example "settings.yaml"
    ```yaml
    credentials:
      redshift-conn:
        type: redshift
        host: my-redshift-cluster.adhfjlasdljfd.eu-west-1.redshift.amazonaws.com
        port: 5439
        user: awsuser
        password: 'Pas$w0rd' #use quotes to avoid conflict with special characters
        dbname: models
    ```

### Connecting with IAM

With an IAM based connection SAYN uses the AWS API to obtain a temporary password to stablish the
connection, so only user, dbname and cluster_id are required.

!!! example "settings.yaml"
    ```yaml
    credentials:
      redshift-conn:
        type: redshift
        cluster_id: my-redshift-cluster
        user: awsuser
        dbname: models
    ```

For this connection type to work:

* `boto3` needs to be installed in the project virtual environment `pip install boto3`.
* The AWS cli need to be [setup](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html#configuration){target="\_blank"}.
* The `user` and `dbname` still need to be specified (use the database user, not the `IAM:user`).
* `host` and `port` can be skipped and these values will be obtained using boto3's `redshift describe-clusters`.

## Redshift specific DDL

# Indexes

Redshift doesn't support index definitions, and so autosql and copy tasks will forbid its definition
in the `ddl` entry in the task definition.

# Sorting

Table sorting can be specified under the `ddl` entry in the task definition

!!! example "tasks/base.yaml"
    ```yaml
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
    ```

With the above example, the table `f_battles` will be sorted by `arena_name` and `fighter1_name`
using a compound key (Redshift default). The type of sorting can be changed to interleaved.

!!! example "tasks/base.yaml"
    ```yaml
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
    ```

For more information, read the latest docs about [SORTKEY](https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_NEW.html){target="\_blank"}.

# Distribution

We can also specify the type of distribution: even, all or key based. If not specified, the Redshift default is even distribution.

!!! example "tasks/base.yaml"
    ```yaml
    tasks:
      f_battles:
        type: autosql
        file_name: f_battles.sql
        materialisation: table
        destination:
          table: f_battles
        ddl:
          distribution: all
    ```

If we want to distribute the table by a given column use the following:

!!! example "tasks/base.yaml"
    ```yaml
    tasks:
      f_battles:
        type: autosql
        file_name: f_battles.sql
        materialisation: table
        destination:
          table: f_battles
        ddl:
          distribution: key(tournament_name)
    ```

For more information, read the latest docs about
[DISTKEY](https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_NEW.html){target="\_blank"}.

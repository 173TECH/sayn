# Redshift

SAYN will consider the following parameters to construct the sqlalchemy url:

- **host**
- **user**
- **password**
- **port**
- **dbname**
- **cluster_id**

The behaviour for Redshift is the same as with PostgreSQL, except that if `cluster_id` is
speciefied, SAYN will use `boto3` to get a temporary password using AWS IAM. For this model to work:

* `boto3` needs to be installed in the project virtual environment `pip install boto3`
* AWS credentials need to be
    [setup](https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-welcome.html)
* The `user` and `dbname` still need to be specified (use the database user, not the `IAM:user`)
* `host` and `port` can be skipped and these values will be obtained using boto3's
    `redshift describe-clusters`

Other parameters specified will be passed on to 
[sqlalchemy.create_engine](https://docs.sqlalchemy.org/en/13/core/engines.html#sqlalchemy.create_engine)
when creating the engine.

!!! example "settings.yaml"
    ```yaml
    ...
    
    credentials:
      postgresql-conn:
        type: redshift
        user: [username]
        dbname: [database_name]
        cluster_id: [redshift-cluster-name]
    
    ...
    ```

Check the sqlalchemy [psycopg2](https://docs.sqlalchemy.org/en/13/dialects/postgresql.html#module-sqlalchemy.dialects.postgresql.psycopg2)
dialect for extra parameters.

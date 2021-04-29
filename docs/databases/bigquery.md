# BigQuery

The BigQuery driver depends on [pybigquery](https://github.com/mxmzdlv/pybigquery){target="\_blank"} and can be
installed with:

```bash
pip install "sayn[bigquery]"
```

The [Bigquery](https://cloud.google.com/bigquery){target="\_blank"} connector looks for the following parameters:

Parameter        | Description                           | Default
---------------- | ------------------------------------- | ---------------------------------------------
project          | GCP project where the cluster is      | Required
credentials_path | Path relative to the project to the json for the service account to use | Required
location         | Default location for tables created   | Dataset default
dataset          | Dataset to use when running queries. Can be specified in sql |

For advanced configurations, SAYN will pass other parameters to `create_engine`, so check the
[pybigquery](https://github.com/mxmzdlv/pybigquery){target="\_blank"}
dialect for extra parameters.

## Bigquery Specific DDL

### Partitioning

SAYN supports specifying the partitioning model for tables created with autosql and copy tasks. To do
this we specify `partition` in the ddl field. The value is a string matching a BigQuery
[partition expression](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-definition-language#partition_expression){target="\_blank"}.

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
          partittion: DATE(_PARTITIONTIME)
    ```

### Clustering

We can also specify the clustering for the table with the `cluster` property in autosql and copy tasks.
The value in this case is a list of columns. If the ddl for the task includes the list of columns, the
columns specified in the `cluster` should be present in the column list.

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
          cluster:
            - arena_name
    ```

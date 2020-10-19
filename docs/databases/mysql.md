# Mysql

The MySQL driver depends on [pymysql](https://github.com/PyMySQL/PyMySQL){target="\_blank"}
and can be installed with:

```bash
pip install "sayn[mysql]"
```

SAYN will consider the following parameters to construct the sqlalchemy url:

Parameter  | Description                           | Default
---------  | ------------------------------------- | --------
host       | Host name or public IP of the server  | Required
port       | Connection port                       | 3306
user       | User name used to connect             | Required
password   | Password for that user                | Required
database   | Database in use upon connection       | Required


Other parameters specified will be passed on to 
[sqlalchemy.create_engine](https://docs.sqlalchemy.org/en/13/core/engines.html#sqlalchemy.create_engine){target="\_blank"}
when creating the engine.

!!! example "settings.yaml"
    ```yaml
    credentials:
      mysql-conn:
        type: mysql
        host: warehouse.company.com
        port: 3306
        user: mysql_user
        password: 'Pas$w0rd' #use quotes to avoid conflict with special characters
        database: models
    ```

Check the sqlalchemy [mysql-connector](https://docs.sqlalchemy.org/en/13/dialects/mysql.html#module-sqlalchemy.dialects.mysql.mysqlconnector){target="\_blank"}
dialect for extra parameters.

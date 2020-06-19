# Mysql

SAYN will consider the following parameters to construct the sqlalchemy url:

- **host**
- **user**
- **password**
- **port**
- **database**

Other parameters specified will be passed on to 
[sqlalchemy.create_engine](https://docs.sqlalchemy.org/en/13/core/engines.html#sqlalchemy.create_engine)
when creating the engine.

!!! example "settings.yaml"
    ```yaml
    ...
    
    credentials:
      mysql-conn:
        type: mysql
        host: [host]
        port: [port]
        user: [username]
        password: '[password]' #use quotes to avoid conflict with special characters
        database: [database_name]
    
    ...
    ```

Check the sqlalchemy [mysql-connector](https://docs.sqlalchemy.org/en/13/dialects/mysql.html#module-sqlalchemy.dialects.mysql.mysqlconnector)
dialect for extra parameters.

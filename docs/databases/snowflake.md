# Snowflake

SAYN will consider the following parameters to construct the sqlalchemy url:

- **account**
- **region**
- **user**
- **password**
- **database**
- **warehouse**
- **role**
- **schema**
- **host**
- **port**

Other parameters specified will be passed on to 
[sqlalchemy.create_engine](https://docs.sqlalchemy.org/en/13/core/engines.html#sqlalchemy.create_engine)
when creating the engine.

!!! example "settings.yaml"
    ```yaml
    ...
    
    credentials:
      postgresql-conn:
        type: snowflake
        account: [account]
        user: [username]
        role: [user_role]
        password: '[password]' #use quotes to avoid conflict with special characters
        database: [database_name]
    
    ...
    ```

Check the sqlalchemy [snowflake dialect](https://docs.snowflake.com/en/user-guide/sqlalchemy.html)
for extra parameters.

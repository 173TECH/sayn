# Databases

## About

SAYN uses `sqlalchemy` in order to manage database connections. It currently supports the following databases:

* Snowflake
* Redshift
* PostgreSQL
* MySQL
* SQLite (for testing only, do not use on production environments)

## Usage

In order to connect to databases, the connection credentials need to be added into the `credentials` sections of the `settings.yaml` file. Please see below examples for how to add `credentials` of each database type:

### Snowfake

```yaml
credentials:
  snowflake-conn:
    type: snowflake
    connect_args:
      account: [account]
      user: [username]
      password: '[password]' #use quotes to avoid conflict with special characters
      database: [database]
      schema: [schema]
      warehouse: [warehouse]
      role: [role]
```

### Redshift

```yaml
credentials:
  redshift-conn:
    type: postgresql
    connect_args:
      host: [host]
      port: [port]
      user: [username]
      password: '[password]' #use quotes to avoid conflict with special characters
      dbname: [database_name]
```

### PostgreSQL

```yaml
credentials:
  postgresql-conn:
    type: postgresql
    connect_args:
      host: [host]
      port: [port]
      user: [username]
      password: '[password]' #use quotes to avoid conflict with special characters
      dbname: [database_name]
```

### MySQL

```yaml
credentials:
  mysql-conn:
    type: mysql
    connect_args:
      host: [host]
      port: [port]
      user: [username]
      password: '[password]' #use quotes to avoid conflict with special characters
      database: [database]
```

### SQLite

```yaml
credentials:
  sqlite-conn:
    type: sqlite
    database: [path_to_database]
```

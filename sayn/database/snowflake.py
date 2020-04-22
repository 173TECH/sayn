import logging

from sqlalchemy import create_engine, Table

from .database import Database


class Snowflake(Database):
    def __init__(self, name, name_in_yaml, yaml):
        self.dialect = "snowflake"
        connection_details = yaml
        connection_details.pop("type")

        self.name = name
        self.name_in_yaml = name_in_yaml

        from snowflake.sqlalchemy import URL
        
        for logger_name in ['snowflake.connector', 'botocore', 'boto3']:
            logger = logging.getLogger(logger_name)
            logger.setLevel(logging.WARNING)

        self.engine = create_engine(URL(**connection_details["connect_args"]))

        self.create_metadata()

    def execute(self, script):
        with self.engine.connect().execution_options(autocommit=True) as connection:
            connection.connection.execute_string(script)

    def _get_schema(self, schema):
        if schema is not None and "." in schema:
            self.engine.execute(f"USE {schema.split('.')[0]}")
            return schema.split('.')[1]

        return schema

    def refresh_metadata(self, only=None, schema=None):
        if schema is not None and "." in schema:
            self.engine.execute(f"USE {schema.split('.')[0]}")
        self.metadata.reflect(only=only, schema=schema, extend_existing=True)

    def get_table(self, table, schema, columns=None):
        """Create a SQLAlchemy Table object. If columns is not None, fills up columns or checks the columns are present"""
        schema = self._get_schema(schema)
        return super(Snowflake, self).get_table(table, schema, columns)

    def move_table(self, src_table, src_schema, dst_table, dst_schema, ddl):
        drop = (
            f"DROP TABLE IF EXISTS {dst_schema+'.' if dst_schema else ''}{dst_table};"
        )
        rename = f"ALTER TABLE {src_schema+'.' if src_schema else ''}{src_table} RENAME TO {dst_schema+'.' if dst_schema else ''}{dst_table};"

        pkey_alter = ""
        if ddl is not None and 'columns' in ddl:
            if len([c for c in ddl["columns"] if c.get("primary", False)]):
                pkey_alter = f"ALTER INDEX {dst_schema+'.' if dst_schema else ''}{src_table}_pkey RENAME TO {dst_table}_pkey;"

        return "\n".join((drop, rename, pkey_alter))


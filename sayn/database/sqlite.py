from sqlalchemy import create_engine

from .database import Database


class Sqlite(Database):
    sql_features = ["CREATE TABLE NO BRACKETS", "NO SET SCHEMA"]

    def __init__(self, name, name_in_settings, settings):
        self.name = name
        self.name_in_settings = name_in_settings
        self.dialect = "sqlite"
        self.db_type = settings.pop("type")
        self.engine = create_engine(f'sqlite:///{settings["database"]}')
        self.create_metadata()

    def execute(self, script):
        with self.engine.connect().execution_options(autocommit=True) as connection:
            connection.connection.executescript(script)

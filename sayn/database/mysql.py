from .database import Database


class Mysql(Database):
    def __init__(self, name, name_in_yaml, yaml):
        self.dialect = "postgresql"
        connection_details = yaml
        connection_details.pop("type")
        super().__init__(name, name_in_yaml, connection_details)

    def execute(self, script):
        with self.engine.connect().execution_options(autocommit=True) as connection:
            connection.execute(script, multi=True)

from .database import Database


class Redshift(Database):
    def __init__(self, name, name_in_yaml, yaml):
        self.dialect = "postgresql"
        connection_details = yaml
        connection_details.pop("type")
        super().__init__(name, name_in_yaml, connection_details)

from .postgresql import Database


class Redshift(Database):
    sql_features = ["DROP CASCADE", "NO SET SCHEMA"]

    def __init__(self, name, name_in_settings, settings):
        self.dialect = "postgresql"
        super().__init__(name, name_in_settings, settings)

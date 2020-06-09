from .database import Database


class Postgresql(Database):
    sql_features = ["DROP CASCADE"]

    def __init__(self, name, name_in_settings, settings):
        self.dialect = "postgresql"
        super().__init__(name, name_in_settings, settings)

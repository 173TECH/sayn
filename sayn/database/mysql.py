from sqlalchemy import create_engine

from .database import Database

db_parameters = ["host", "user", "password", "port", "database"]


class Mysql(Database):
    sql_features = ["DROP CASCADE"]

    def __init__(self, name, name_in_settings, settings):
        db_type = settings.pop("type")

        # Create engine using the connect_args argument to create_engine
        if "connect_args" not in settings:
            settings["connect_args"] = dict()
        for param in db_parameters:
            if param in settings:
                if param == "port":
                    value = int(settings.pop(param))
                else:
                    value = settings.pop(param)
                settings["connect_args"][param] = value

        engine = create_engine("mysql+pymysql://", **settings)
        self.setup_db(name, name_in_settings, db_type, engine)

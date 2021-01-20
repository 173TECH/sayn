from sqlalchemy import create_engine

# from sqlalchemy.types import Boolean, DateTime, Text

from . import Database

db_parameters = ["host", "user", "password", "port", "database"]


class Mysql(Database):
    def _feature(self, feature):
        return feature in ("CAN REPLACE VIEW",)

    def create_engine(self, settings):
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

        return create_engine("mysql+pymysql://", **settings)

    def execute(self, script):
        for s in script.split(";"):
            if len(s.strip()) > 0:
                self.engine.execute(s)

    def move_table(self, src_table, dst_table, src_schema=None, dst_schema=None, **ddl):
        template = self._jinja_env.get_template("move_table_mysql.sql")

        return template.render(
            src_schema=src_schema,
            src_table=src_table,
            dst_schema=dst_schema,
            dst_table=dst_table,
            **ddl,
        )

    # def _transform_column_type(self, column_type, dialect):
    #     ctype = column_type.compile().lower()
    #     if ctype == "tinyint(1)":
    #         return Boolean().compile(dialect=dialect)
    #     elif ctype == "datetime":
    #         return DateTime().compile(dialect=dialect)
    #     elif ctype == "longtext":
    #         return Text().compile(dialect=dialect)
    #     elif ctype == "mediumtext":
    #         return Text().compile(dialect=dialect)
    #     elif "varchar" in ctype:
    #         return Text().compile(dialect=dialect)
    #     else:
    #         return super()._transform_column_type(column_type, dialect)

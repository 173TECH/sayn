# import importlib
# from pathlib import Path

# from ..utils.singleton import singleton
from . import DatabaseError
from .postgresql import Postgresql
from .sqlite import Sqlite
from .mysql import Mysql
from .redshift import Redshift
from .snowflake import Snowflake

drivers = {
    "postgresql": Postgresql,
    "sqlite": Sqlite,
    "mysql": Mysql,
    "snowflake": Snowflake,
    "redshift": Redshift,
}


def create_all(credentials):
    return {n: create(n, c["name_in_yaml"], c["yaml"]) for n, c in credentials.items()}


def create(name, name_in_yaml, yaml):
    db_type = yaml["type"]
    if db_type not in drivers:
        raise DatabaseError(f"No driver for {db_type} found")
    else:
        return drivers[db_type](name, name_in_yaml, yaml)


# Dynamic loading currently not working
# @singleton
# class DatabaseCreator:
#     def __init__(self):
#         self.drivers = [
#             f.name[:-3]
#             for f in Path(__file__).parent.glob("*.py")
#             if f.is_file() and f.name not in ("__init__.py", "database.py", 'creator.py')
#         ]
#         self.creators = dict()
#
#     def create_all(self, credentials):
#         return {n: self.create(n, c['name_in_yaml'], c['yaml']) for n, c in credentials.items()}
#
#     def create(self, name, name_in_yaml, yaml):
#         db_type = yaml["type"].data
#         if db_type not in self.drivers:
#             raise DatabaseError(f"No driver for {db_type} found")
#         else:
#             if db_type not in self.creators:
#                 #mod = __import__("sayn.database."+db_type, fromlist=)
#                 #mod = importlib.import_module(db_type, package="sayn.database")
#                 if hasattr(mod, db_type.capitalize()):
#                     raise DatabaseError(
#                         f"Missing class {db_type.capitalize()} in module {db_type}"
#                     )
#                 else:
#                     self.creators[db_type] = getattr(mod, db_type.capitalize())
#
#             return self.creators[db_type](name, name_in_yaml, yaml)

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
    return {
        n: create(n, c["name_in_settings"], c["settings"])
        for n, c in credentials.items()
    }


def create(name, name_in_settings, settings):
    db_type = settings["type"]
    if db_type not in drivers:
        raise DatabaseError(f"No driver for {db_type} found")
    else:
        return drivers[db_type](name, name_in_settings, settings)

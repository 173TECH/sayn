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

db_params = ("max_batch_rows", "type")


def create_all(credentials):
    return {
        n: create(n, c["name_in_settings"], c["settings"])
        for n, c in credentials.items()
    }


def create(name, name_in_settings, settings):
    db_type = settings.pop("type")
    if db_type not in drivers:
        raise ValueError(f"No driver for {db_type} found")
    else:
        # Extract common parameters
        common_params = {k: v for k, v in settings.items() if k in db_params}
        settings = {k: v for k, v in settings.items() if k not in db_params}

        db_obj = drivers[db_type](name, name_in_settings, db_type, common_params)
        db_obj._set_engine(db_obj.create_engine(settings))

        return db_obj

from .bigquery import Bigquery
from .mysql import Mysql
from .postgresql import Postgresql
from .redshift import Redshift
from .sqlite import Sqlite
from .snowflake import Snowflake
from .unknown import UnknownDb

drivers = {
    "postgresql": Postgresql,
    "sqlite": Sqlite,
    "mysql": Mysql,
    "snowflake": Snowflake,
    "redshift": Redshift,
    "bigquery": Bigquery,
}

db_params = ("max_batch_rows", "type")


def create(name, name_in_settings, settings):
    db_type = settings.pop("type")
    if db_type not in drivers:
        raise ValueError(f"No driver for {db_type} found")
    else:
        # Extract common parameters
        common_params = {k: v for k, v in settings.items() if k in db_params}
        settings = {k: v for k, v in settings.items() if k not in db_params}

        db_obj = drivers[db_type](
            name,
            name_in_settings,
            db_type,
            common_params,
            settings,
        )

        return db_obj


def create_dummy(name):
    return UnknownDb(name, name, "dummy", dict(), dict())

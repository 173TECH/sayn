from datetime import datetime, date, timedelta
import importlib
import logging
import os
from pathlib import Path
import shutil
import sys

from jinja2 import Environment, FileSystemLoader, StrictUndefined

from .utils import yaml
from .utils.singleton import singleton
from .utils.logger import Logger
from . import database


class SaynConfigError(Exception):
    pass


# TODO remove this
# default_log_file = Path("logs/sayn.log")


@singleton
class Config:
    logs_path = Path("logs")  # Default log folder

    def __init__(
        self,
        profile=None,
        full_load=False,
        start_dt=None,
        end_dt=None,
        debug=False,
        **cmd_parameters,
    ):
        # Process basic command parameters
        self.run_id = (datetime.now() - datetime(1970, 1, 1)).total_seconds()

        # For dates we default to yesterday
        yesterday = date.today() - timedelta(days=1)
        if start_dt is None:
            start_dt = yesterday
        elif type(start_dt) == str:
            start_dt = datetime.strptime(start_dt, "%Y-%m-%d")

        if end_dt is None:
            end_dt = yesterday
        elif type(end_dt) == str:
            end_dt = datetime.strptime(end_dt, "%Y-%m-%d")

        if end_dt < start_dt:
            raise SaynConfigError("end-dt can't be earlier than start-dt")

        self.options = {
            "full_load": full_load,
            "start_dt": start_dt,
            "end_dt": end_dt,
            "debug": debug,
        }

        # Read the config
        config = self.read_config(profile)
        if config is None:
            raise SaynConfigError("Error detected in configuration")

        # Process and store the config from the yaml files
        self.options.update(
            {"selected_profile": config["selected_profile"],}
        )

        self.setup_parameters(config["parameters"], cmd_parameters)

        self.setup_credentials(
            config["default_db"], config["db_credentials"], config["api_credentials"]
        )

        self.setup_tasks(config["tasks"], config["groups"], config["additional_models"])
        if self._task_definitions is None:
            raise SaynConfigError(f"Error reading tasks")

    #
    # Read config files
    #
    def read_config(self, profile):
        models = self.read_models()
        if models is None:
            return

        settings = self.read_settings(
            profile, models["required_credentials"], list(models["parameters"].keys()),
        )
        if settings is None:
            return

        parameters = models["parameters"]
        parameters.update(settings["parameters"])

        # Validate the default_db against db credentials
        if models["default_db"] not in settings["db_credentials"]:
            default_cred = settings["api_credentials"][models["default_db"]]
            logging.error(
                f"Credential {default_cred['name_in_yaml']} is not a database"
            )
            return

        return {
            "selected_profile": settings["selected_profile"],
            "parameters": parameters,
            "default_db": models["default_db"],
            "db_credentials": settings["db_credentials"],
            "api_credentials": settings["api_credentials"],
            "groups": models["groups"],
            "tasks": models["tasks"],
            "additional_models": models["additional_models"],
        }

    def read_models(self):
        path = Path("models.yaml")
        parsed = yaml.load(path)
        if parsed is None:
            return

        schema = yaml.Map(
            {
                "sayn_project_name": yaml.NotEmptyStr(),
                yaml.Optional("models"): yaml.UniqueSeq(yaml.NotEmptyStr()),
                # TODO remove this
                # yaml.Optional("folders"): yaml.Map(
                #     {
                #         yaml.Optional(p): yaml.NotEmptyStr()
                #         for p in ("sql", "python", "logs", "compile", "models")
                #     }
                # ),
                yaml.Optional("parameters"): yaml.MapPattern(
                    yaml.NotEmptyStr(), yaml.Any()
                ),
                yaml.Optional("default_db"): yaml.NotEmptyStr(),
                "required_credentials": yaml.UniqueSeq(yaml.NotEmptyStr()),
                yaml.Optional("groups"): yaml.MapPattern(
                    yaml.NotEmptyStr(), yaml.MapPattern(yaml.NotEmptyStr(), yaml.Any()),
                ),
                yaml.Optional("tasks"): yaml.MapPattern(
                    yaml.NotEmptyStr(), yaml.MapPattern(yaml.NotEmptyStr(), yaml.Any()),
                ),
            }
        )

        try:
            parsed.revalidate(schema)
        except yaml.ValidationError as e:
            # Setup file logger with default log file
            # TODO remove this
            # Logger().set_file_logger(default_log_file)
            logging.error(f"Error reading {path.name}")
            logging.error(e)
            return

        # TODO remove this
        # At this point we have the paths, so setup the file logger
        # self.setup_paths(**parsed.data.get("folders", dict()))
        # Logger().set_file_logger(log_file=Path(self.logs_path, "sayn.log"))
        self.setup_paths()

        # Read additional models
        additional_models = dict()
        for model_name in parsed.data.get("models", []):
            model = self.read_additional_model(model_name)
            if model is None:
                return

            additional_models[model_name] = {
                "model_name": model_name,
                "tasks": model["tasks"],
                "groups": model.get(
                    "groups", yaml.as_document(dict(), yaml.EmptyDict())
                ),
            }

        # Figure out default_db
        default_db = parsed.data.get("default_db")

        if default_db is None and len(parsed["required_credentials"]) == 1:
            default_db = parsed["required_credentials"].data[0]
        elif default_db is None and len(parsed["required_credentials"]) > 1:
            logging.error(
                "Default database is required when multiple credentials are set"
            )
            return
        elif default_db not in parsed["required_credentials"]:
            logging.error(f'Default database "{default_db}" not defined')
            return
        # else:
        #    default_db already points to an existing credential

        return {
            "parameters": parsed.data.get("parameters", dict()),
            "default_db": default_db,
            "required_credentials": parsed.data["required_credentials"],
            "groups": parsed["groups"]
            if "groups" in parsed
            else yaml.as_document(dict(), yaml.EmptyDict()),
            "tasks": parsed["tasks"]
            if "tasks" in parsed
            else yaml.as_document(dict(), yaml.EmptyDict()),
            "additional_models": additional_models,
        }

    def read_additional_model(self, model):
        path = Path(self.models_path, f"{model}.yaml")
        parsed = yaml.load(path)
        if parsed is None:
            return

        schema = yaml.Map(
            {
                yaml.Optional("groups"): yaml.MapPattern(
                    yaml.NotEmptyStr(), yaml.MapPattern(yaml.NotEmptyStr(), yaml.Any()),
                ),
                "tasks": yaml.MapPattern(
                    yaml.NotEmptyStr(), yaml.MapPattern(yaml.NotEmptyStr(), yaml.Any()),
                ),
            }
        )

        try:
            parsed.revalidate(schema)
        except yaml.ValidationError as e:
            # Setup file logger with default log file
            # TODO remove this
            # Logger().set_file_logger(default_log_file)
            logging.error(f'Error reading additional model "{path.name}"')
            logging.error(e)
            return

        return parsed

    def read_settings(self, profile, required_credentials, allowed_parameters):
        path = Path("settings.yaml")
        parsed = yaml.load(path)
        if parsed is None:
            return

        schema = yaml.Map(
            {
                yaml.Optional("default_profile"): yaml.NotEmptyStr(),
                "profiles": yaml.MapPattern(
                    yaml.NotEmptyStr(),  # profile name
                    yaml.Map(
                        {
                            "credentials": yaml.Map(
                                {c: yaml.NotEmptyStr() for c in required_credentials}
                            ),
                            yaml.Optional("parameters"): yaml.MapPattern(
                                yaml.Enum(allowed_parameters), yaml.Any()
                            ),
                        }
                    ),
                ),
                "credentials": yaml.MapPattern(
                    yaml.NotEmptyStr(), yaml.MapPattern(yaml.NotEmptyStr(), yaml.Any())
                ),
            }
        )

        try:
            parsed.revalidate(schema)
        except yaml.ValidationError as e:
            logging.error(f'Error reading settings "{path.name}"')
            logging.error(e)
            return

        # Extract values on first pass
        selected_profile = profile or parsed.data.get("default_profile")

        # Get the default_profile
        if selected_profile is None and len(parsed["profiles"]) > 1:
            logging.error("Default profile is required when multiple profiles are set")
            return
        elif selected_profile is None and len(parsed["profiles"]) == 1:
            selected_profile = list(parsed["profiles"].data.keys())[0]
        elif selected_profile not in parsed["profiles"]:
            logging.error(f'Default profile "{selected_profile}" not defined')
            return

        profile = parsed["profiles"][selected_profile]

        # Keeping only the credentials specified in the profile
        credentials = {
            n: {"name_in_yaml": n, "yaml": c}
            for n, c in parsed.data["credentials"].items()
        }
        no_type = [n for n, c in credentials.items() if "type" not in c["yaml"]]
        if len(no_type) > 0:
            logging.error(f"Some credentials have no type: {', '.join(no_type)}")
            return
        credentials = {
            n: credentials[yn] for n, yn in profile.data["credentials"].items()
        }

        return {
            "selected_profile": selected_profile,
            "parameters": profile.data.get("parameters", dict()),
            "db_credentials": {
                n: c for n, c in credentials.items() if c["yaml"]["type"] != "api"
            },
            "api_credentials": {
                n: {k: v for k, v in c["yaml"].items() if k != "type"}
                for n, c in credentials.items()
                if c["yaml"]["type"] == "api"
            },
        }

    #
    # Setup
    #
    def setup_paths(self, **paths):
        self.models_path = Path(paths.get("models", "models"))
        self.sql_path = Path(paths.get("sql", "sql"))
        self.python_path = Path(paths.get("python", "python"))

        self.compile_path = Path(paths.get("compile", "compile"))
        if self.compile_path.exists():
            if self.compile_path.is_dir():
                shutil.rmtree(self.compile_path.absolute())
            else:
                self.compile_path.unlink()

        # self.logs_path = Path(paths.get("logs", "logs"))
        # self.logs_path.mkdir(parents=True, exist_ok=True)

    def setup_parameters(self, from_config, from_cmd):
        self.parameters = from_config
        self.parameters.update(from_cmd)

        start_dt = self.options["start_dt"]
        start_dt = start_dt.strftime("%Y-%m-%d") if type(start_dt) != str else start_dt

        end_dt = self.options["end_dt"]
        end_dt = end_dt.strftime("%Y-%m-%d") if type(end_dt) != str else end_dt

        self.parameters.update(
            {
                "start_dt": f"'{start_dt}'",
                "end_dt": f"'{end_dt}'",
                "full_load": self.options["full_load"],
            }
        )

        self.jinja_env = Environment(
            loader=FileSystemLoader(os.getcwd()),
            undefined=StrictUndefined,
            keep_trailing_newline=True,
        )
        self.jinja_env.globals.update(**self.parameters)

    def setup_credentials(self, default_db, db_credentials, api_credentials):
        self.dbs = database.create_all(db_credentials)
        self.default_db = self.dbs[default_db]
        self.api_credentials = api_credentials

    def setup_python_tasks_module(self):
        path = Path(self.python_path, "__init__.py")
        if not path.is_file():
            raise SaynConfigError(f"Missing file: {path.fullname}")

        loader = importlib.machinery.SourceFileLoader(
            "python_tasks", str(Path(self.python_path, "__init__.py"))
        )
        spec = importlib.util.spec_from_loader("python_tasks", loader)
        m = None
        m = importlib.util.module_from_spec(spec)

        sys.modules[spec.name] = m

        try:
            loader.exec_module(m)
        except Exception as e:
            logging.error(f"Error importing python module")
            logging.error(e)
            raise SaynConfigError(
                f"Error importing python folder {self.python_path.absolute}"
            )

    def setup_tasks(self, tasks, groups, additional_models):
        def get_tasks(tasks, groups, all_models_tasks, model_name=None):
            if len(tasks) == 0:
                return dict()

            # Basic checks on tasks:
            # 1. Parents are tasks existing in all_models_tasks
            # 2. Groups also exists in the "tasks" map
            all_groups = list(groups.data.keys())

            def value_validator(k, v):
                if k == "group":
                    return yaml.Enum(all_groups)
                elif k == "parents":
                    return yaml.UniqueSeq(yaml.Enum(all_models_tasks))
                else:
                    return yaml.Any()

            for name, task in tasks.items():
                try:
                    schema = {k: value_validator(k, v) for k, v in task.data.items()}
                    task.revalidate(yaml.Map(schema))
                except yaml.ValidationError as e:
                    logging.error(f'Error in task "{str(name)}"reading tasks')
                    logging.error(e)
                    return

            # Create the task definition dictionary
            tasks = {
                t: {
                    "task": tasks[t],
                    "group": groups[tasks[t]["group"]]
                    if "group" in tasks[t]
                    else yaml.as_document(dict(), yaml.EmptyDict()),
                    "type": tasks[t].get("type"),
                    "model": model_name,
                }
                for t in tasks.data.keys()
            }

            # Consolidate the type
            for task in tasks.values():
                if task["type"] is None and task["group"] is not None:
                    task["type"] = task["group"].get("type")

            no_type = [n for n, t in tasks.items() if t["type"] is None]
            if len(no_type) > 0:
                logging.error(f"Some tasks have no type: {', '.join(no_type)}")
                return

            return tasks

        # we set task_names_all_models object to validate parents and allow for cross-model parents
        all_models_tasks = list(tasks.data.keys())
        for _, model_params in additional_models.items():
            all_models_tasks += model_params["tasks"].data.keys()

        if len(all_models_tasks) == 0:
            logging.error("No tasks defined")
            self._task_definitions = None
            return

        tasks = get_tasks(tasks, groups, all_models_tasks)
        if tasks is None:
            self._task_definitions = None
            return

        for name, model in additional_models.items():
            additional_tasks = get_tasks(
                model["tasks"], model["groups"], all_models_tasks, name
            )
            if additional_tasks is None:
                self._task_definitions = None
                return
            elif len(set(tasks.keys()).intersection(additional_tasks.keys())) > 0:
                logging.error(
                    f"Some tasks are duplicated across models: {', '.join(set(tasks.keys()).intersection(additional_tasks.keys()))}"
                )
                self._task_definitions = None
                return
            else:
                tasks.update(additional_tasks)

        if len([n for n, t in tasks.items() if t["type"] == "python"]) > 0:
            self.setup_python_tasks_module()

        self._task_definitions = tasks

from datetime import datetime, date, timedelta
import importlib
import json
import logging
import os
from pathlib import Path
import shutil
import sys

from jinja2 import Environment, FileSystemLoader, StrictUndefined

from .utils import yaml
from .utils.singleton import singleton
from . import database


class SaynConfigError(Exception):
    pass


@singleton
class Config:
    def __init__(
        self,
        profile=None,
        full_load=False,
        start_dt=None,
        end_dt=None,
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

        self.setup_credentials(config["default_db"], config["credentials"])

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

        return {
            "selected_profile": settings["selected_profile"],
            "parameters": parameters,
            "default_db": models["default_db"],
            "credentials": settings["credentials"],
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
            logging.error(f"Error reading {path.name}")
            logging.error(e)
            return

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
            logging.error(f'Error reading additional model "{path.name}"')
            logging.error(e)
            return

        return parsed

    def read_settings(self, profile, required_credentials, allowed_parameters):
        env_vars = {k: v for k, v in os.environ.items() if k.startswith("SAYN_")}
        if len(env_vars) > 0:
            print("Reading settings from environment variables")
            return self.read_settings_environment(
                env_vars, required_credentials, allowed_parameters
            )
        else:
            print('Reading settings from "settings.yaml"')
            return self.read_settings_file(
                profile, required_credentials, allowed_parameters
            )

    def read_settings_environment(
        self, env_vars, required_credentials, allowed_parameters
    ):
        parameters = dict()
        credentials = dict()

        for key, value in env_vars.items():
            if key.startswith("SAYN_PARAMETER_"):
                parameter_key = key[len("SAYN_PARAMETER_") :]
                if parameter_key not in allowed_parameters:
                    logging.error(
                        f'Parameter "{parameter_key}" not defined in models.yaml. '
                    )
                    logging.error(
                        f"Allowed parameters are {', '.join(allowed_parameters)}"
                    )
                    return
                try:
                    # Try to interpret it as a json
                    parameters[key] = json.loads(value)
                except:
                    # Treat it as a string
                    parameters[parameter_key] = value

            elif key.startswith("SAYN_CREDENTIAL_"):
                credential_key = key[len("SAYN_CREDENTIAL_") :]
                if credential_key not in required_credentials:
                    continue
                try:
                    # Needs to be a json as the base structure for credentials is a map
                    credentials[credential_key] = {
                        "name_in_settings": None,
                        "settings": json.loads(value),
                    }
                except Exception as e:
                    logging.error(
                        f'Error trying to parse environment variable "{key}" as json'
                    )
                    logging.error(e)
                    return

            else:
                logging.error(f'Environment variable "{key}" is not recognised')
                return

            # Ensure all credentials are specified
            for credential in required_credentials:
                if credential not in credentials:
                    logging.error(f'Missing credential "{credential}"')
                    return

        return {
            "selected_profile": None,
            "parameters": parameters,
            "credentials": credentials,
        }

    def read_settings_file(self, profile, required_credentials, allowed_parameters):
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
            n: {"name_in_settings": n, "settings": c}
            for n, c in parsed.data["credentials"].items()
        }
        no_type = [n for n, c in credentials.items() if "type" not in c["settings"]]
        if len(no_type) > 0:
            logging.error(f"Some credentials have no type: {', '.join(no_type)}")
            return
        credentials = {
            name: credentials[name_in_settings]
            for name, name_in_settings in profile.data["credentials"].items()
        }

        return {
            "selected_profile": selected_profile,
            "parameters": profile.data.get("parameters", dict()),
            "credentials": credentials,
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

    def setup_credentials(self, default_db, credentials):
        print()
        print([c['settings'].keys() for _,c in credentials.items()])
        print()
        db_credentials = {
            n: c for n, c in credentials.items() if c["settings"]["type"] != "api"
        }
        self.api_credentials = {
            n: {k: v for k, v in c["settings"].items() if k != "type"}
            for n, c in credentials.items()
            if c["settings"]["type"] == "api"
        }

        # Validate the default_db against db credentials
        if default_db not in db_credentials:
            logging.error(
                f"Credential {credentials[default_db]['name_in_settings']} is not a database"
            )
            return

        self.dbs = database.create_all(db_credentials)
        self.default_db = self.dbs[default_db]

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

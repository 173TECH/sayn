import copy
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
    _task_definitions = None

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
        config = self._read_config(profile)
        if config is None:
            raise SaynConfigError("Error detected in configuration")

        # Process and store the config from the yaml files
        self.options.update(
            {"selected_profile": config["selected_profile"],}
        )

        self._setup_parameters(config["parameters"], cmd_parameters)

        self._setup_credentials(config["default_db"], config["credentials"])

        self._setup_tasks(config["presets"], config["groups"])
        if self._task_definitions is None:
            raise SaynConfigError(f"Error reading tasks")

    ###############################
    # Reader methods
    ###############################

    def _read_config(self, profile):
        models = self._read_project()
        if models is None:
            return

        settings = self._read_settings(
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
            "presets": models["presets"],
            "groups": models["groups"],
        }

    def _read_project(self):
        path = Path("project.yaml")
        parsed = yaml.load(path)
        if parsed is None:
            return

        groups = {g.name[: -len(g.suffix)]: None for g in Path("groups").glob("*.yaml")}

        schema = yaml.Map(
            {
                yaml.Optional("parameters"): yaml.MapPattern(
                    yaml.NotEmptyStr(), yaml.Any()
                ),
                "required_credentials": yaml.UniqueSeq(yaml.NotEmptyStr()),
                yaml.Optional("default_db"): yaml.NotEmptyStr(),
                yaml.Optional("presets"): yaml.MapPattern(
                    yaml.NotEmptyStr(), yaml.MapPattern(yaml.NotEmptyStr(), yaml.Any()),
                ),
                yaml.Optional("groups"): yaml.UniqueSeq(yaml.Enum(groups.keys())),
            }
        )

        try:
            parsed.revalidate(schema)
        except yaml.ValidationError as e:
            logging.error(f"Error reading {path.name}")
            logging.error(e)
            return

        self._setup_paths()

        # Read groups
        groups = {name: self._read_group(name) for name in groups.keys()}
        if len([v for v in groups.values() if v is None]) > 0:
            # If any errors reading groups, fail the reading
            return

        # Determine default_db
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
            "required_credentials": parsed.data["required_credentials"],
            "default_db": default_db,
            "presets": parsed.data.get("presets", dict()),
            "groups": groups,
        }

    def _read_group(self, group_name):
        path = Path(self.groups_path, f"{group_name}.yaml")
        parsed = yaml.load(path)
        if parsed is None:
            return

        schema = yaml.Map(
            {
                yaml.Optional("presets"): yaml.MapPattern(
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
            logging.error(f'Error reading group "{path.name}"')
            logging.error(e)
            return

        return {
            "presets": parsed.data.get("presets", dict()),
            "tasks": parsed.data.get("tasks", dict()),
        }

    def _read_settings(self, profile, required_credentials, allowed_parameters):
        env_vars = {k: v for k, v in os.environ.items() if k.startswith("SAYN_")}
        if len(env_vars) > 0:
            print("Reading settings from environment variables")
            return self._read_settings_environment(
                env_vars, required_credentials, allowed_parameters
            )
        else:
            print('Reading settings from "settings.yaml"')
            return self._read_settings_file(
                profile, required_credentials, allowed_parameters
            )

    def _read_settings_environment(
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

    def _read_settings_file(self, profile, required_credentials, allowed_parameters):
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

    ###############################
    # Setup config object methods
    ###############################

    def _setup_paths(self, **paths):
        self.groups_path = Path(paths.get("groups", "groups"))
        self.sql_path = Path(paths.get("sql", "sql"))
        self.python_path = Path(paths.get("python", "python"))

        self.compile_path = Path(paths.get("compile", "compile"))
        if self.compile_path.exists():
            if self.compile_path.is_dir():
                shutil.rmtree(self.compile_path.absolute())
            else:
                self.compile_path.unlink()

    def _setup_parameters(self, from_config, from_cmd):
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

    def _setup_credentials(self, default_db, credentials):
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

    ###############################
    # Task processing methods
    ###############################

    def _setup_python_tasks_module(self):
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

    def _get_group_presets(self, group_presets, group_name, global_presets):
        presets = copy.deepcopy(group_presets)
        for preset_name, preset in presets.items():
            if preset.get("preset") is not None:
                # If the preset references a preset, embed it
                if preset["preset"] in global_presets:
                    referenced_preset = copy.deepcopy(global_presets[preset["preset"]])

                    # Merge common fields to all task types the outer preset
                    if "parameters" in referenced_preset:
                        if "parameters" not in preset:
                            preset["parameters"] = dict()
                        preset["parameters"].update(
                            {
                                k: v
                                for k, v in referenced_preset.pop("parameters").items()
                                if k not in preset["parameters"]
                            }
                        )

                    if "parents" in referenced_preset:
                        if "parents" not in preset:
                            preset["parents"] = list()
                        preset["parents"].extend(referenced_preset.pop("parents"))

                    # Add the rest of the global preset to the preset
                    preset["preset"] = referenced_preset

                else:
                    # Global preset not defined
                    logging.error(
                        "Referenced preset {} in group {} not defined in project.yaml".format(
                            preset.get("preset"), group_name
                        )
                    )
                    return

        out_presets = copy.deepcopy(global_presets)
        out_presets.update(presets)
        return out_presets

    def _get_group_tasks(self, tasks, group_name, presets, all_tasks):
        tasks = copy.deepcopy(tasks)
        for name, task in tasks.items():
            # Check for duplicates
            if name in all_tasks:
                logging.error(
                    "Duplicate tasks in group {}: {}".format(
                        group_name,
                        ", ".join(set(tasks.keys().intersection(set(all_tasks)))),
                    )
                )
                return

            # Embed the preset in the task definition
            if "preset" in task:
                if task["preset"] in presets:
                    task["preset"] = presets[task["preset"]]
                else:
                    logging.error(
                        "Preset {} referenced in task {} not found".format(
                            task["preset"], name
                        )
                    )
                    return

            # Consolidate the type of the task
            if "type" not in task:
                if "type" in task.get("preset", dict()):
                    # Try to take it from the preset
                    task["type"] = task["preset"]["type"]
                elif "preset" in task.get("preset", dict()):
                    # Nested preset
                    task["type"] = task["preset"]["preset"]["type"]
                else:
                    logging.error(f"Missing type in task {name} in group {group_name}")
                    return

            # Consolidate the parent list
            if "parents" not in task and "parents" in task.get("preset", dict()):
                task['parents'] = list()
                task["parents"].extend(task["preset"]["parents"])
                task["parents"] = list(set(task["parents"]))

        return tasks

    def _setup_tasks(self, global_presets, groups):
        tasks = dict()

        for group_name, group in groups.items():
            presets = self._get_group_presets(
                group["presets"], group_name, global_presets
            )

            # Now process the tasks

            # Add tasks to the output list
            group_tasks = self._get_group_tasks(
                group["tasks"], group_name, presets, list(tasks.keys())
            )
            if group_tasks is None:
                #import IPython;IPython.embed()
                return
            tasks.update(group_tasks)

        # Now that we have all tasks, check parents referenced in each task exists in the list of all tasks
        # and check if we need to load the python module
        load_python = False
        for name, task in tasks.items():
            if "parents" in task:
                missing_parents = [
                    parent
                    for parent in task.get("parents", list())
                    if parent not in tasks
                ]
                if len(missing_parents) > 0:
                    logging.error(
                        "Missing parent tasks {} referenced in task {}".format(
                            ", ".join(missing_parents), name
                        )
                    )
                    return

            if task["type"] == "python":
                load_python = True

        if load_python:
            self._setup_python_tasks_module()

        self._task_definitions = tasks

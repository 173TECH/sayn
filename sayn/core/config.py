from copy import deepcopy
import json
import os
from pathlib import Path
import re
import shutil
from typing import List, Optional, Dict, Any

from pydantic import BaseModel, validator, ValidationError
from ruamel.yaml import YAML
from ruamel.yaml.error import MarkedYAMLError

from ..database.creator import create as create_db
from ..utils.misc import merge_dicts, merge_dict_list
from ..utils.dag import upstream, topological_sort
from .errors import Err, Exc, Ok

RE_ENV_VAR_NAME = re.compile(r"SAYN_(?P<type>PARAMETER|CREDENTIAL)_(?P<name>.*)")


def cleanup_compilation(folder):
    compile_path = Path(folder)
    if compile_path.exists():
        if compile_path.is_dir():
            shutil.rmtree(compile_path.absolute())
        else:
            compile_path.unlink()

    compile_path.mkdir()


def read_yaml_file(filename):
    try:
        contents = Path(filename).read_text()
    except:
        return Err("parsing", "read_file_error", filename=filename)

    try:
        return Ok(YAML().load(contents))
    except MarkedYAMLError as e:
        return Exc(e, filename=filename)


def is_unique(field_name, v):
    if len(set(v)) != len(v):
        raise ValueError(f"Duplicate values found in {field_name}")
    return v


class Project(BaseModel):
    required_credentials: List[str]
    default_db: Optional[str]
    parameters: Optional[Dict[str, Any]] = dict()
    presets: Optional[Dict[str, Dict[str, Any]]] = dict()
    groups: List[str] = []

    @validator("required_credentials")
    def required_credentials_are_unique(cls, v):
        return is_unique("required_credentials", v)

    @validator("default_db", always=True)
    def default_db_exists(cls, v, values, **kwargs):
        if v is None and len(values["required_credentials"]) == 1:
            return values["required_credentials"][0]
        elif v is None:
            raise ValueError("Missing default_db in project.yaml")
        elif v not in values["required_credentials"]:
            raise ValueError(f'default_db value "{v}" not in required_credentials')

        return v

    @validator("groups", pre=True, always=True)
    def set_groups(cls, v):
        if not os.path.isdir(Path("tasks")):
            raise ValueError("'tasks' folder cannot be found in project directory.")

        groups = [f.name[:-5] for f in Path("tasks").glob("*.yaml")]

        if len(groups) == 0:
            raise ValueError(
                "No YAML file found in the tasks folder. Make sure you are using .yaml extension."
            )

        return groups


def read_project():
    result = read_yaml_file(Path("project.yaml"))
    if result.is_err:
        return result

    try:
        return Ok(Project(**result.value))
    except ValidationError as e:
        return Exc(e, where="read_project")


class TaskGroup(BaseModel):
    presets: Optional[Dict[str, Dict[str, Any]]] = dict()
    tasks: Optional[Dict[str, Dict[str, Any]]]


def read_groups(groups):
    out = dict()
    for name in groups:
        result = read_yaml_file(Path("tasks", f"{name}.yaml"))
        if result.is_err:
            return result
        else:
            try:
                out[name] = TaskGroup(**result.value)
            except ValidationError as e:
                return Exc(e, where="read_tasks")

    return Ok(out)


class Settings(BaseModel):
    class Environment(BaseModel):
        parameters: Optional[Dict[str, Any]]
        credentials: Optional[Dict[str, Dict[str, Any]]]

    class SettingsYaml(BaseModel):
        class Profile(BaseModel):
            parameters: Optional[Dict[str, Any]]
            credentials: Dict[str, str]

        credentials: Dict[str, dict]
        profiles: Dict[str, Profile]
        default_profile: Optional[str]

        @validator("profiles")
        def yaml_credentials(cls, v, values, **kwargs):
            if v is None:
                raise ValueError("No profiles defined in settings.yaml.")
            for profile_name, profile in v.items():
                for project_name, settings_name in profile.credentials.items():
                    if "credentials" not in values:
                        raise ValueError("No credentials defined in settings.yaml.")

                    if settings_name not in values["credentials"]:
                        raise ValueError(
                            f'"{settings_name}" in profile "{profile_name}" not declared in credentials.'
                        )

            return v

        @validator("default_profile", always=True)
        def default_profile_exists(cls, v, values, **kwargs):
            if "profiles" not in values:
                # We're running this always so that we can default to the first profile
                # so we cover the case of no profile specified, but the error will be
                # covered by the profiles validation
                return v

            if v is None and len(values["profiles"]) > 1:
                raise ValueError(
                    'Can\'t determine default profile. Use "default_profile" to specify it.'
                )
            elif v is None:
                return list(values["profiles"].keys())[0]

            if "profiles" not in values:
                raise ValueError("No profiles defined in settings.yaml.")
            elif v not in values["profiles"].keys():
                raise ValueError(f'default_profile "{v}" not in the profiles map.')

            return v

        def get_profile_info(self, profile_name=None):
            profile_name = profile_name or self.default_profile

            if profile_name not in self.profiles:
                raise ValueError(f'Profile "{profile_name}" not in settings.yaml.')

            return {
                "parameters": self.profiles[profile_name].parameters,
                "credentials": {
                    cred_project_name: self.credentials[cred_settings_name]
                    for cred_project_name, cred_settings_name in self.profiles[
                        profile_name
                    ].credentials.items()
                },
            }

    yaml: Optional[SettingsYaml]
    environment: Optional[Environment]

    def get_settings(self, profile_name=None):
        if profile_name is not None and self.yaml is None:
            return Err("get_settings", "missing_settings_yaml")
        elif self.yaml is not None:
            out = self.yaml.get_profile_info(profile_name)
        else:
            out = {"credentials": dict(), "parameters": dict()}

        if profile_name is None and self.environment is not None:
            # When no profile is specified, and there's something in the environment,
            # we try to use environment variables
            if self.environment.parameters is not None:
                out["parameters"].update(self.environment.parameters)
            if self.environment.credentials is not None:
                out["credentials"].update(self.environment.credentials)

        return Ok(out)


def read_settings():
    environment = {"parameters": dict(), "credentials": dict()}
    for name, value in os.environ.items():
        name = RE_ENV_VAR_NAME.match(name)
        if name is not None:
            name = name.groupdict()

            environment[name["type"].lower() + "s"][name["name"]] = YAML().load(value)

    environment = {k: v for k, v in environment.items() if len(v) > 0}
    if len(environment) == 0:
        environment = None

    filepath = Path("settings.yaml")
    if filepath.exists():
        result = read_yaml_file(filepath)
        if result.is_err:
            return result
        else:
            settings_yaml = result.value
    else:
        settings_yaml = None

    try:
        if settings_yaml is not None:
            return Ok(Settings(yaml=settings_yaml, environment=environment))
        else:
            return Ok(Settings(environment=environment))
    except ValidationError as e:
        return Exc(e, where="settings_reading")


###############################
# Connections functions
###############################


def get_connections(credentials):
    try:
        return Ok(
            {
                name: create_db(name, name, deepcopy(config))
                if config["type"] != "api"
                else {k: v for k, v in config.items() if k != "type"}
                for name, config in credentials.items()
            }
        )
    except Exception as e:
        return Exc(e)


###############################
# Task related config functions
###############################


def get_presets(global_presets, groups):
    """Returns a dictionary of presets merged with the referenced preset

    Presets define a direct acyclic graph by including the `preset` property, so
    this function validates that there are no cycles and that all referenced presets
    are defined.

    In the output, preset names are prefixed with `sayn_global:` or `group:` so that we can
    merge all presets in the project in the same dictionary.

    Args:
      global_presets (dict): dictionary containing the presets defined in project.yaml
      groups (sayn.app.config.TaskGroup): a list of task groups from the tasks/ folder
    """
    # 1. Construct a dictionary of presets so we can attach that info to the tasks
    presets_info = {
        f"sayn_global:{k}": {kk: vv for kk, vv in v.items() if kk != "preset"}
        for k, v in global_presets.items()
    }

    # 1.1. We start with the global presets defined in project.yaml
    presets_project = {
        k: [f"sayn_global:{v}"] if v is not None else []
        for k, v in {
            f"sayn_global:{name}": preset.get("preset")
            for name, preset in global_presets.items()
        }.items()
    }

    # 1.2. Then we add the presets defined in the task groups
    for group_name, group in groups.items():
        presets_info.update(
            {
                f"{group_name}:{k}": {kk: vv for kk, vv in v.items() if kk != "preset"}
                for k, v in group.presets.items()
            }
        )

        group_presets = {
            name: preset.get("preset") for name, preset in group.presets.items()
        }

        # Check if the preset referenced is defined in the task group, otherwise, point at the
        # global task group
        group_presets = {
            f"{group_name}:{k}": [
                f"{group_name}:{v}"
                if v in group_presets and v != k
                else f"sayn_global:{v}"
            ]
            if v is not None
            else []
            for k, v in group_presets.items()
        }
        presets_project.update(group_presets)

    # 1.3. The preset references represent a dag that we need to validate, ensuring
    #      there are no cycles and that all references exists
    result = topological_sort(presets_project)
    if result.is_err:
        return result
    else:
        topo_sort = result.value

    # 1.4. Merge the presets with the reference preset, so that we have 1 dictionary
    #      per preset a task could reference
    presets = {
        name: merge_dict_list(
            [presets_info[p] for p in upstream(presets_project, name).value]
            + [presets_info[name]]
        )
        for name in topo_sort
    }

    return Ok(presets)


def get_task_dict(task, task_name, group_name, presets):
    """Returns a single task merged with the referenced preset

    Args:
      task (dict): a dictionary with the task information
      task_name (str): the name of the task
      group_name (str): the name of the group it appeared on
      presets (dict): a dictionary of merged presets returned by get_presets
    """
    if "preset" in task:
        preset_name = task["preset"]
        preset = presets.get(
            f"{group_name}:{preset_name}",
            presets.get(f"sayn_global:{preset_name}"),
        )
        if preset is None:
            return Err(
                "get_task_dict",
                "missing_preset",
                group=group_name,
                task=task_name,
                preset=preset_name,
            )
        task = merge_dicts(preset, task)

    return Ok(dict(task, name=task_name, group=group_name))


def get_tasks_dict(global_presets, groups):
    """Returns a dictionary with the task definition with the preset information merged

    Args:
      global_presets (dict): a dictionary with the presets as defined in project.yaml
      groups (sayn.common.config.TaskGroup): a list of task groups from the tasks/ folder
    """
    result = get_presets(global_presets, groups)
    if result.is_err:
        return result
    else:
        presets = result.value

    errors = dict()
    tasks = dict()
    for group_name, group in groups.items():
        if group.tasks is not None:
            for task_name, task in group.tasks.items():
                if task_name in tasks:
                    return Err(
                        "dag",
                        "duplicate_task",
                        task=task_name,
                        groups=(group_name, tasks[task_name]["group"]),
                    )
                result = get_task_dict(task, task_name, group_name, presets)
                if result.is_ok:
                    tasks[task_name] = result.value
                else:
                    errors[task_name] = result.error

    if len(errors) > 0:
        return Err("get_tasks_dict", "task_parsing_error", errors=errors)
    else:
        return Ok(tasks)

from copy import deepcopy
import os
from pathlib import Path
import re
from typing import Any, Mapping, Optional

from pydantic import BaseModel, validator, ValidationError, Extra
from ruamel.yaml import YAML
from ruamel.yaml.error import MarkedYAMLError

from ..database.creator import create as create_db
from .errors import Err, Exc, Ok

RE_ENV_VAR_NAME = re.compile(
    r"SAYN_((?P<stringify>(DATABASE|SCHEMA|TABLE)_(PREFIX|SUFFIX|STRINGIFY))"
    r"|(?P<type>PARAMETER|CREDENTIAL)_(?P<name>.+))$"
)


class Environment(BaseModel):
    class Stringify(BaseModel):
        database_prefix: Optional[str]
        database_suffix: Optional[str]
        database_stringify: Optional[str]
        schema_prefix: Optional[str]
        schema_suffix: Optional[str]
        schema_stringify: Optional[str]
        table_prefix: Optional[str]
        table_suffix: Optional[str]
        table_stringify: Optional[str]

    parameters: Optional[Mapping[str, Any]]
    credentials: Optional[Mapping[str, Mapping[str, Any]]]
    stringify: Optional[Stringify]

    class Config:
        extra = Extra.forbid
        anystr_lower = True


class SettingsYaml(BaseModel):
    class Profile(BaseModel):
        parameters: Optional[Mapping[str, Any]]
        credentials: Mapping[str, str]

        database_prefix: Optional[str]
        database_suffix: Optional[str]
        database_stringify: Optional[str]
        schema_prefix: Optional[str]
        schema_suffix: Optional[str]
        schema_stringify: Optional[str]
        table_prefix: Optional[str]
        table_suffix: Optional[str]
        table_stringify: Optional[str]

        class Config:
            extra = Extra.forbid
            anystr_lower = True

    credentials: Mapping[str, Mapping]
    profiles: Mapping[str, Profile]
    default_profile: Optional[str]

    class Config:
        extra = Extra.forbid
        anystr_lower = True

    @validator("profiles")
    def yaml_credentials(cls, v, values):
        if v is None:
            raise ValueError("No profiles defined in settings.yaml.")
        for profile_name, profile in v.items():
            for settings_name in profile.credentials.values():
                if "credentials" not in values:
                    raise ValueError("No credentials defined in settings.yaml.")

                if settings_name not in values["credentials"]:
                    raise ValueError(
                        f'"{settings_name}" in profile "{profile_name}" not declared in credentials.'
                    )

        return v

    @validator("default_profile", always=True)
    def default_profile_exists(cls, v, values):
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
        if profile_name is not None:
            profile_name = profile_name
        else:
            profile_name = self.default_profile

        if profile_name not in self.profiles:
            raise ValueError(f'Profile "{profile_name}" not in settings.yaml.')

        profile_info = self.profiles[profile_name]

        return {
            "parameters": profile_info.parameters,
            "credentials": {
                cred_project_name: self.credentials[cred_settings_name]
                for cred_project_name, cred_settings_name in profile_info.credentials.items()
            },
            "stringify": {
                k: v
                for k, v in {
                    f"{obj_type}_{str_type}": profile_info.dict()[
                        f"{obj_type}_{str_type}"
                    ]
                    for obj_type in ("database", "schema", "table")
                    for str_type in ("prefix", "suffix", "stringify")
                }.items()
                if v is not None
            },
        }


def read_settings():
    # First read the settings.yaml
    filepath = Path("settings.yaml")
    if filepath.exists():
        try:
            yaml_content = YAML().load(Path(filepath).read_text())
        except MarkedYAMLError as e:
            return Exc(e, filename=filepath)

        try:
            settings_yaml = SettingsYaml(**yaml_content)
        except ValidationError as e:
            return Exc(e, where="settings_reading")
    else:
        settings_yaml = None

    # Process the environment variables
    environment = {"parameters": dict(), "credentials": dict(), "stringify": dict()}
    for name, value in os.environ.items():
        name = RE_ENV_VAR_NAME.match(name)
        if name is not None:
            name = name.groupdict()

            if name["type"] is not None:
                environment[name["type"].lower() + "s"][name["name"]] = YAML().load(
                    value
                )
            else:
                environment["stringify"][name["stringify"].lower()] = value

    environment = {k: v for k, v in environment.items() if len(v) > 0}
    try:
        settings_env = Environment(**environment)
    except ValidationError as e:
        return Exc(e, where="settings_reading")

    if len(environment) == 0:
        environment = None

    return Ok({"yaml": settings_yaml, "env": settings_env})


def get_settings(yaml, environment, profile_name=None):
    if profile_name is not None and yaml is None:
        return Err("get_settings", "missing_settings_yaml")
    elif yaml is not None:
        out = yaml.get_profile_info(profile_name)
    else:
        out = {"credentials": dict(), "parameters": dict(), "stringify": dict()}

    if profile_name is None and environment is not None:
        # When no profile is specified, and there's something in the environment,
        # we try to use environment variables
        if environment.parameters is not None:
            out["parameters"].update(environment.parameters)

        if environment.credentials is not None:
            out["credentials"].update(environment.credentials)

        if environment.stringify is not None:
            out["stringify"].update(
                {k: v for k, v in environment.stringify if v is not None}
            )

    return Ok(out)


def get_connections(credentials, stringify, prod_stringify):
    import IPython

    IPython.embed()
    try:
        return Ok(
            {
                name: create_db(name, name, deepcopy(config), stringify, prod_stringify)
                if config["type"] != "api"
                else {k: v for k, v in config.items() if k != "type"}
                for name, config in credentials.items()
            }
        )
    except Exception as e:
        return Exc(e)

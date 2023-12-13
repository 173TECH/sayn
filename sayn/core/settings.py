from copy import deepcopy
import os
from pathlib import Path
import re
from typing import Any, Mapping, Optional, Sequence

from pydantic import BaseModel, validator, ValidationError, Extra
from ruamel.yaml import YAML
from ruamel.yaml.error import MarkedYAMLError

from ..database.creator import create as create_db, create_dummy
from .errors import Err, Exc, Ok

RE_ENV_VAR_NAME = re.compile(
    r"SAYN_((?P<stringify>(DATABASE|SCHEMA|TABLE)_(PREFIX|SUFFIX|OVERRIDE))"
    r"|(?P<from_prod>FROM_PROD)$"
    r"|(?P<default_run>DEFAULT_RUN)$"
    r"|(?P<type>PARAMETER|CREDENTIAL)_(?P<name>.+))$"
)

RE_DEFAULT_RUN_VAL = re.compile(
    r"^( *(?:(?:-t|--tasks|-x|--exclude)(?: +(?:group\:|tag\:)?[a-zA-Z][a-zA-Z_0-9]+)+|-u|--upstream-prod))+$"
)
RE_DEFAULT_RUN = re.compile(
    r" *((?:-t|--tasks|-x|--exclude)(?: +(?:group\:|tag\:)?[a-zA-Z][a-zA-Z_0-9]+)+|-u|--upstream-prod)"
)


class TableGlob(str):
    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v):
        if not isinstance(v, str):
            raise TypeError("string required")
        m = re.match(
            r"((?P<schema>[a-zA-Z\*][_0-9a-zA-Z\*]*).)?(?P<table>[a-zA-Z\*][_0-9a-zA-Z\*]*)",
            v,
        )
        if not m:
            raise ValueError("invalid table specification")

        out = ""
        if m.groupdict()["schema"] is not None:
            out += m.groupdict()["schema"].replace("*", ".*") + r"\."
        if m.groupdict()["table"] is not None:
            out += m.groupdict()["table"].replace("*", ".*")

        return cls(out)


class Environment(BaseModel):
    class Stringify(BaseModel):
        database_prefix: Optional[str]
        database_suffix: Optional[str]
        database_override: Optional[str]
        schema_prefix: Optional[str]
        schema_suffix: Optional[str]
        schema_override: Optional[str]
        table_prefix: Optional[str]
        table_suffix: Optional[str]
        table_override: Optional[str]

    parameters: Optional[Mapping[str, Any]]
    credentials: Optional[Mapping[str, Mapping[str, Any]]]
    stringify: Optional[Stringify]
    from_prod: Optional[Sequence[TableGlob]]
    default_run: Optional[str]

    class Config:
        extra = Extra.forbid
        anystr_lower = True

    @validator("default_run")
    def default_run_validator(cls, v):
        m = RE_DEFAULT_RUN_VAL.match(v)
        if m is None:
            raise ValueError(
                f'Invalid default_run specification "{v}". Allowed arguments: -t/--tasks, -x/--exclude and -u/--upstream-prod'
            )

        include = set()
        exclude = set()
        upstream_prod = None
        for arg in RE_DEFAULT_RUN.findall(v):
            arg = arg.strip()
            if arg.startswith("-t") or arg.startswith("--tasks"):
                include.update(arg.split(" ")[1:])
            elif arg.startswith("-x") or arg.startswith("--exclude"):
                exclude.update(arg.split(" ")[1:])
            elif arg.startswith("-u") or arg.startswith("--upstream-prod"):
                upstream_prod = True
            else:
                raise ValueError('Incorrect option in default_run "{arg}"')

        return {
            "include": include,
            "exclude": exclude,
            "upstream_prod": upstream_prod,
        }


class SettingsYaml(BaseModel):
    class Profile(BaseModel):
        parameters: Optional[Mapping[str, Any]]
        credentials: Mapping[str, str]

        database_prefix: Optional[str]
        database_suffix: Optional[str]
        database_override: Optional[str]
        schema_prefix: Optional[str]
        schema_suffix: Optional[str]
        schema_override: Optional[str]
        table_prefix: Optional[str]
        table_suffix: Optional[str]
        table_override: Optional[str]
        from_prod: Optional[Sequence[TableGlob]]
        default_run: Optional[str]

        class Config:
            extra = Extra.forbid
            anystr_lower = True

        @validator("default_run")
        def default_run_validator(cls, v):
            m = RE_DEFAULT_RUN_VAL.match(v)
            if m is None:
                raise ValueError(
                    f'Invalid default_run specification "{v}". Allowed arguments: -t/--tasks, -x/--exclude and -u/--upstream-prod'
                )

            include = set()
            exclude = set()
            upstream_prod = None
            for arg in RE_DEFAULT_RUN.findall(v):
                arg = arg.strip()
                if arg.startswith("-t") or arg.startswith("--tasks"):
                    include.update(arg.split(" ")[1:])
                elif arg.startswith("-x") or arg.startswith("--exclude"):
                    exclude.update(arg.split(" ")[1:])
                elif arg.startswith("-u") or arg.startswith("--upstream-prod"):
                    upstream_prod = True
                else:
                    raise ValueError('Incorrect option in default_run "{arg}"')

            return {
                "include": include,
                "exclude": exclude,
                "upstream_prod": upstream_prod,
            }

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
        if profile_name is None:
            profile_name = self.default_profile

        if profile_name not in self.profiles:
            raise ValueError(f'Profile "{profile_name}" not in settings.yaml.')

        if profile_name is None:
            raise ValueError("Unknown profile")

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
                    for str_type in ("prefix", "suffix", "override")
                }.items()
                if v is not None
            },
            "from_prod": [f for f in profile_info.from_prod or list()],
            "default_run": profile_info.default_run
            or {"include": set(), "exclude": set(), "upstream_prod": None},
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
    environment = {
        "parameters": dict(),
        "credentials": dict(),
        "stringify": dict(),
        "from_prod": list(),
    }
    for name, value in os.environ.items():
        matched_name = RE_ENV_VAR_NAME.match(name)
        if matched_name is not None:
            matched_name = matched_name.groupdict()

            if matched_name["type"] is not None:
                environment[matched_name["type"].lower() + "s"][
                    matched_name["name"]
                ] = YAML().load(value)
            elif matched_name["from_prod"] is not None:
                environment["from_prod"] = [v.strip() for v in value.split(",")]
            elif matched_name["default_run"] is not None:
                environment["default_run"] = value
            else:
                environment["stringify"][matched_name["stringify"].lower()] = value

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
        out = {
            "credentials": dict(),
            "parameters": dict(),
            "stringify": dict(),
            "from_prod": list(),
            "default_run": {"include": set(), "exclude": set(), "upstream_prod": None},
        }

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

        if environment.from_prod is not None:
            out["from_prod"] = environment.from_prod

        if environment.default_run is not None:
            out["default_run"] = environment.default_run

    return Ok(out)


def get_connections(credentials):
    out = dict()
    for name, config in credentials.items():
        try:
            if config is None:
                out[name] = create_dummy(name)
            elif config["type"] == "api":
                out[name] = {k: v for k, v in config.items() if k != "type"}
            else:
                out[name] = create_db(
                    name,
                    name,
                    deepcopy(config),
                )
        except Exception as e:
            return Exc(e)

    return Ok(out)

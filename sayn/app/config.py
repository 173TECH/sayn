from pathlib import Path
from typing import List, Optional, Dict, Any

from pydantic import BaseModel, validator
from ruamel.yaml import YAML
from ruamel.yaml.error import MarkedYAMLError


def read_yaml_file(filename):
    yaml = YAML()

    try:
        contents = Path(filename).read_text()
    except:
        raise ConfigError(f"{filename} not found or could not be read")

    try:
        parsed = yaml.load(contents)
    except MarkedYAMLError as e:
        raise YamlParsingError(e.problem, "project.yaml", e.problem_mark.line + 1)

    return parsed


class ConfigError(Exception):
    pass


class YamlParsingError(ConfigError):
    def __init__(self, problem, file, line):
        message = f"Error parsing {file}: {problem} on line {line}"
        super(ConfigError, self).__init__(message)

        self.problem = problem
        self.file = file
        self.line = line


def is_unique(field_name, v):
    if len(set(v)) != len(v):
        raise ConfigError(f"Duplicate values found in {field_name}")
    return v


class Project(BaseModel):
    required_credentials: List[str]
    default_db: Optional[str]
    presets: Optional[Dict[str, Dict[str, Any]]]
    dags: List[str]

    @validator("required_credentials")
    def required_credentials_are_unique(cls, v):
        return is_unique("required_credentials", v)

    @validator("dags")
    def dags_are_unique(cls, v):
        return is_unique("dags", v)

    @validator("default_db")
    def default_db_exists(cls, v, values, **kwargs):
        if v not in values["required_credentials"]:
            raise ConfigError(f'default_db value "{v}" not in required_credentials')
        return v


def read_project():
    return Project(**read_yaml_file(Path("project.yaml")))


class Dag(BaseModel):
    presets: Optional[Dict[str, Dict[str, Any]]]
    tasks: Dict[str, Dict[str, Any]]


def read_dags(dags):
    return {name: Dag(**read_yaml_file(Path("dags", f"{name}.yaml"))) for name in dags}


class Settings(BaseModel):
    pass


def read_settings():
    pass


def validate(config, settings):
    pass

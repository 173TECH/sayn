from pydantic import ValidationError
from ruamel.yaml import YAML
from ruamel.yaml.error import MarkedYAMLError

from ..core.errors import SaynMissingFileError, SaynParsingError


def read_yaml_file(file, Model):
    if not file.exists():
        raise SaynMissingFileError(str(file))

    try:
        parsed = YAML().load(file.read_text())
    except MarkedYAMLError as exc:
        raise SaynParsingError(
            "yaml_parsing",
            [
                {
                    "file_name": str(file),
                    "line": exc.problem_mark.line,
                    "column": exc.problem_mark.column,
                    "message": exc.problem,
                    "snippet": exc.problem_mark.get_snippet(),
                }
            ],
        )

    try:
        return Model(**parsed)
    except ValidationError as exc:
        errors = list()
        for error in exc.errors():
            tmp_parsed = parsed
            for k in error["loc"][:-1]:
                tmp_parsed = tmp_parsed[k]
            lc = tmp_parsed.lc.key(error["loc"][-1])

            errors.append(
                {
                    "file_name": str(file),
                    "loc": error["loc"],
                    "line": lc[0] + 1,
                    "column": lc[1] + 1,
                    "message": error["msg"],
                }
            )

        raise SaynParsingError("data_validation", errors)

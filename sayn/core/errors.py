from typing import Any, Dict

from pydantic import ValidationError
from ruamel.yaml.error import MarkedYAMLError
import sqlalchemy


class Error:
    kind: str
    code: str
    details: Dict = dict()

    def __init__(self, kind, code, details):
        self.kind = kind
        self.code = code
        self.details = details

    def __repr__(self):
        return f"Result.Err ({self.kind}::{self.code}): {self.details.__repr__()}"


class Result:
    is_ok: bool = False
    value: Any = None
    error: Error = None

    def __init__(self, value: Any = None, error: Error = None):
        if error is not None:
            self.is_ok = False
            self.error = error
        else:
            self.is_ok = True
            self.value = value

    def __repr__(self):
        if self.is_ok:
            return f"Result.Ok: {self.value.__repr__()}"
        else:
            return self.error.__repr__()

    @property
    def is_err(self):
        return not self.is_ok

    def mk_error_message(self, errors):
        """Returns a list of error messages from a Error result

        Args:
          errors (List[Result]): a list of results
        """
        pass


def Ok(value=None):
    """Creates an OK result"""
    return Result(value=value)


def Err(kind, code, **details):
    """Creates an Error result"""
    return Result(error=Error(kind, code, details))


def Exc(exc, **kwargs):
    """Creates an Error result from an exception"""
    if isinstance(exc, MarkedYAMLError):
        # Ruamel error
        # TODO check that it's controlled from sayn.core.config
        return Result(
            error=Error(
                "parsing",
                "yaml_parse",
                dict(
                    **kwargs,
                    **{"error": exc.problem, "line": exc.problem_mark.line + 1},
                ),
            )
        )

    elif isinstance(exc, ValidationError):
        # Pydantic error
        return Result(
            error=Error("parsing", "validation_error", {"errors": exc.errors()})
        )

    elif isinstance(exc, NotImplementedError) and exc.args[0] == "SAYN task":
        # Missing implementation for SAYN command
        return Result(
            error=Error(
                "exception",
                "not_implemented",
                {"class": exc.args[1], "method": exc.args[2]},
            )
        )
    elif isinstance(exc, NotImplementedError):
        # Other missing method
        return Result(
            error=Error("exception", "unknown_not_implemented", {"exception": exc})
        )

    # TODO add other exceptions like:
    #   - DB errors
    #   - Jinja
    elif isinstance(exc, sqlalchemy.exc.OperationalError):
        return Result(
            error=Error(
                "database",
                "operational_error",
                {"exception": exc, "message": " ".join(exc.args)},
            )
        )

    else:
        return Result(
            error=Error("exception", "unhandled_exception", {"exception": exc})
        )


class DBError(Exception):
    db_name = None
    db_type = None

    def __init__(self, db_name, db_type, *args, **kwargs):
        self.db_name = db_name
        self.db_type = db_type

        super(sqlalchemy.SQLAlchemyError, self).__init__(*args, **kwargs)

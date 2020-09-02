from pydantic import ValidationError
from ruamel.yaml.error import MarkedYAMLError


def mk_error(cls, kind, code, details):
    return cls(False, None, kind, code, details)


class Result:
    is_ok = False
    value = None
    error = None

    def __init__(self, is_ok, value, error):
        self.is_ok = is_ok
        self.value = value
        self.error = error

    @classmethod
    def Ok(cls, value=None):
        """Creates an OK result"""
        return cls(True, value, None)

    @classmethod
    def Err(cls, kind, code, details):
        """Creates an Error result"""
        return cls(False, None, {"kind": kind, "code": code, "details": details})

    @classmethod
    def Exc(cls, exc, **kwargs):
        """Creates an Error result from an exception"""
        if isinstance(exc, MarkedYAMLError):
            # Ruamel error
            # TODO check that it's controlled from sayn.core.config
            return mk_error(
                cls,
                "parsing",
                "yaml_parse",
                dict(
                    **kwargs,
                    **{"error": exc.problem, "line": exc.problem_mark.line + 1},
                ),
            )

        elif isinstance(exc, ValidationError):
            # Pydantic error
            return mk_error(
                cls, "parsing", "validation_error", {"errors": exc.errors()},
            )

        elif isinstance(exc, NotImplementedError) and exc.args[0] == "SAYN task":
            # Missing implementation for SAYN command
            return mk_error(
                cls,
                "exception",
                "not_implemented",
                {"class": exc.args[1], "method": exc.args[2]},
            )
        elif isinstance(exc, NotImplementedError):
            # Other missing method
            return mk_error(
                cls, "exception", "unknown_not_implemented", {"exception": exc},
            )

        # TODO add other exceptions like:
        #   - DB errors
        #   - Jinja

        else:
            # Unhandled exception

            return mk_error(
                cls, "exception", "unhandled_exception", {"exception": exc},
            )

    @property
    def is_err(self):
        return not self.is_ok

    def mk_error_message(self, errors):
        """Returns a list of error messages from a Error result

        Args:
          errors (List[Result]): a list of results
        """
        pass


class DagMissingParentsError(Exception):
    def __init__(self, missing):
        message = "Some referenced tasks are missing: " + ", ".join(
            [
                f'"{parent}" referenced by ({", ".join(children)})'
                for parent, children in missing.items()
            ]
        )
        super(DagMissingParentsError, self).__init__(message)

        self.missing = missing


class DagCycleError(Exception):
    def __init__(self, cycle):
        message = f"Cycle found in the DAG {' -> '.join(cycle)}"
        super(DagCycleError, self).__init__(message)

        self.cycle = cycle


class ConfigError(Exception):
    pass


class DatabaseError(Exception):
    pass


class TaskCreationError(Exception):
    pass

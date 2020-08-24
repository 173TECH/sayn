class Result:
    is_ok = False
    value = None
    error = None

    def __init__(self, is_ok, value, error):
        self.is_ok = is_ok
        self.value = value
        self.error = error

    @classmethod
    def Err(cls, kind, code, details):
        return cls(False, None, {"kind": kind, "code": code, "details": details})

    @classmethod
    def Ok(cls, value=None):
        return cls(True, value, None)

    @property
    def is_err(self):
        return not self.is_ok


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


class YamlParsingError(ConfigError):
    def __init__(self, problem, file, line):
        message = f"Error parsing {file}: {problem} on line {line}"
        super(ConfigError, self).__init__(message)

        self.problem = problem
        self.file = file
        self.line = line


class DatabaseError(Exception):
    pass


class TaskCreationError(Exception):
    pass

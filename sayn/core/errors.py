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


class CommandError(Exception):
    pass


class YamlParsingError(ConfigError):
    def __init__(self, problem, file, line):
        message = f"Error parsing {file}: {problem} on line {line}"
        super(ConfigError, self).__init__(message)

        self.problem = problem
        self.file = file
        self.line = line


class PythonLoaderError(Exception):
    pass


class TaskQueryError(Exception):
    pass


class DatabaseError(Exception):
    pass


class TaskError(Exception):
    pass


class TaskCreationError(TaskError):
    pass

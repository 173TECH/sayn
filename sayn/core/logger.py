from pathlib import Path
from datetime import datetime
import subprocess

from colorama import init, Fore, Style
import humanize

init(autoreset=True)

try:
    from importlib import metadata
except ImportError:
    import importlib_metadata as metadata


class TaskLogger:
    _logger = None
    _task_name = None
    steps = list()
    current_step = None

    def __init__(self, logger, task_name):
        self._logger = logger
        self.task_name = task_name

    def define_steps(self, steps):
        self.steps = steps

    def set_current_step(self, step):
        if step not in self.step:
            raise ValueError(
                f"{step} not in defined steps. Use `self.logger.define_steps(list)` first."
            )
        elif self.current_step is not None:
            self._logger._report_event(
                level="info", event="finish_step", step=self.current_step
            )

        self.current_step = step
        self._logger._report_event(level="info", event="start_step", step=step)

    def debug(self, message, step=None, details=None):
        self._report_event(level="debug", message=message, step=step, details=details)

    def info(self, message, step=None, details=None):
        self._report_event(level="info", message=message, step=step, details=details)

    def warning(self, message, step=None, details=None):
        self._report_event(level="warning", message=message, step=step, details=details)

    def error(self, message, step=None, details=None):
        self._report_event(level="error", message=message, step=step, details=details)

    def _report_event(self, **kwargs):
        event = {k: v for k, v in kwargs.items() if v is not None}
        if "event" not in event and "message" in event:
            event["event"] = "message"
        else:
            event["event"] = "unknown"
        event["ts"] = datetime.now()
        self._logger.report_event(event)


class Logger:
    def write_event(self, event):
        raise NotImplementedError()


class ConsoleLogger(Logger):
    level = "debug"
    levels = ("success", "error", "warning", "info", "debug")
    fields = {
        "run_id": {},
        "sayn_version": {},
        "project_git_commit": {},
        "project_name": {},
        "ts": {"include": "prefix"},
        "level": {"include": "prefix", "transform": lambda x: x.upper()[:3]},
        "stage": {"include": "prefix"},
        "event": {"include": "prefix"},
        "message": {"include": "details"},
        "details": {},
        "duration": {
            "include": "details",
            "transform": lambda x: f'Took {humanize.naturaldelta(x, minimum_unit="microseconds")}',
        },
    }

    def __init__(self, level):
        self.level = level

    def write_event(self, event):
        style = self.get_colours(event)
        prefix_fields = list()
        details_fields = list()
        for field, conf in self.fields.items():
            if len(conf) > 0 and field in event:
                value = f'{conf.get("transform", lambda x: x)(event[field])}'
                if conf.get("include") == "prefix":
                    prefix_fields.append(value)
                elif conf.get("include") == "details":
                    details_fields.append(value)

        self.print(f"{'|'.join(prefix_fields)}|{'|'.join(details_fields)}", style)

    def get_colours(self, event):
        if event["level"] == "success":
            return Fore.GREEN
        elif event["level"] == "error":
            return Fore.RED
        elif event["level"] == "warning":
            return Fore.YELLOW
        elif event["level"] == "debug":
            return Style.DIM
        else:
            return Style.RESET_ALL

    def print(self, line, style):
        print(style + line)


class AppLogger:
    loggers = dict()
    stage = None
    run_id = None
    tasks = list()
    current_task = None
    sayn_version = metadata.version("sayn")
    project_git_commit = None
    project_name = Path(".").absolute().name

    def __init__(self, run_id, loggers=dict()):
        self.run_id = run_id
        self.stage = "setup"
        self.loggers = loggers
        try:
            self.project_git_commit = (
                subprocess.check_output(["git", "rev-parse", "HEAD"])
                .split()[0]
                .decode("utf-8")
            )
        except:
            pass

        # if log_file is not None:
        #     self.loggers["file"] = FileLogger(
        #         run_id=run_id, debug=debug, log_file=log_file
        #     )

    def set_stage(self, stage):
        self.stage = stage

    def set_tasks(self, tasks):
        self.tasks = tasks

    def set_current_task(self, task):
        self.current_task = task

    def get_task_logger(self, task_name):
        return TaskLogger(self, task_name)

    def report_event(self, event):
        if "event" not in event:
            raise ValueError(f'event key missing in "{event}"')
        else:
            event.update(
                dict(
                    run_id=self.run_id,
                    stage=self.stage,
                    sayn_version=self.sayn_version,
                    project_git_commit=self.project_git_commit,
                    project_name=self.project_name,
                )
            )
            for logger in self.loggers.values():
                logger.write_event(event)

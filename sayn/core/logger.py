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
        elif "event" not in event:
            event["event"] = "unknown"
        event["context"] = "task"
        event["task"] = self.task_name
        self._logger.report_event(event)


class Logger:
    def report_event(self, event):
        raise NotImplementedError()


class ConsoleDebugLogger(Logger):
    level = "debug"
    levels = ("success", "error", "warning", "info", "debug")
    fields = {
        "run_id": {"include": "prefix"},
        "project_name": {"include": "prefix"},
        "sayn_version": {},
        "project_git_commit": {},
        "ts": {"include": "prefix"},
        "stage": {"include": "prefix"},
        "level": {"include": "details", "transform": lambda x: x.upper()},
        "event": {},
        "task": {},
        "message": {"include": "details"},
        "details": {},
        "duration": {
            "include": "details",
            "transform": lambda x: f'Took {humanize.precisedelta(x, minimum_unit="seconds")}',
        },
    }

    def __init__(self, level):
        self.level = level

    def report_event(self, event):
        if self.levels.index(event["level"]) > self.levels.index(self.level):
            return
        prefix = self.get_prefix(event)
        lines = self.get_lines(event)
        style = self.get_colours(event)

        for line in lines:
            self.print(f"{prefix}|{line}", style)

    def get_prefix(self, event):
        prefix_fields = list()
        for field, conf in self.fields.items():
            if conf.get("include") != "prefix":
                continue
            if len(conf) > 0 and field in event:
                value = f'{conf.get("transform", lambda x: x)(event[field])}'
                prefix_fields.append(value)

        return f"{'|'.join(prefix_fields)}"

    def get_lines(self, event):
        if event["context"] == "task":
            prefix = "|".join(
                (
                    f"({event['task_order']}/{event['total_tasks']})"
                    if event["stage"] != "setup"
                    else "",
                    "INFO" if event["level"] == "success" else event["level"].upper(),
                    event["task"],
                )
            )
            if event["event"] == "start_task":
                return [f"{prefix}|Starting."]
            elif event["event"] == "finish_task":
                if event["level"] == "error":
                    return [
                        f"{prefix}|{event['message']}",
                        f"{prefix}|{event['duration']}",
                    ]
                else:
                    return [f"{prefix}|{event['duration']}"]
            elif event["event"] == "cannot_run":
                return [f"{prefix}|SKIPPING"]
        elif event["event"] == "execution_finished":
            prefix = f"{event['level'].upper()}|"
            out = [
                ". ".join(
                    (
                        f"{prefix}Process finished",
                        f"Total tasks: {len(event['succeeded']+event['skipped']+event['failed'])}",
                        f"Success: {len(event['succeeded'])}",
                        f"Failed {len(event['failed'])}",
                        f"Skipped {len(event['skipped'])}.",
                    )
                )
            ]

            for status in ("succeeded", "failed", "skipped"):
                if len(event[status]) > 0:
                    out.append(
                        (
                            f"The following tasks "
                            f"{'were ' if status == 'skipped' else ''}{status}: "
                            f"{', '.join(event[status])}"
                        )
                    )

            out.append(
                f"{event['command'].capitalize()} took {humanize.precisedelta(event['duration'])}"
            )
            return out

        return list()

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
    current_task_n = 0
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
        if event["context"] == "task":
            if event["task"] != self.current_task:
                self.current_task_n = self.tasks.index(event["task"]) + 1

            event["total_tasks"] = len(self.tasks)
            event["task_order"] = self.current_task_n

        if "event" not in event:
            event["event"] = "unknown"
        elif event["event"] == "execution_finished":
            event["level"] = (
                "error"
                if len(event["failed"]) > 0
                else "warning"
                if len(event["skipped"]) > 0
                else "success"
            )

        event.update(
            dict(
                run_id=self.run_id,
                stage=self.stage,
                sayn_version=self.sayn_version,
                project_git_commit=self.project_git_commit,
                project_name=self.project_name,
                ts=datetime.now(),
            )
        )

        for logger in self.loggers.values():
            logger.report_event(event)

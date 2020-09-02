from pathlib import Path
from datetime import datetime
import subprocess

from ..tasks.task_logger import TaskLogger

try:
    from importlib import metadata
except ImportError:
    import importlib_metadata as metadata


class EventTracker:
    loggers = list()
    stage = None
    run_id = None
    current_stage = None
    tasks = list()
    current_task = None
    current_task_n = 0
    sayn_version = metadata.version("sayn")
    project_git_commit = None
    project_name = Path(".").absolute().name

    def __init__(self, run_id, loggers=list()):
        self.run_id = run_id
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

    def register_logger(self, logger):
        self.loggers.append(logger)

    def start_stage(self, stage, details=None):
        self.current_stage = stage
        self.report_event(
            context="app", event="start_stage", stage=stage, details=details,
        )

    def finish_current_stage(self, duration, task_statuses, details):
        self.report_event(
            context="app",
            event="finish_stage",
            stage=self.current_stage,
            task_statuses=task_statuses,
            duration=duration,
        )
        self.current_stage = None

    def set_tasks(self, tasks):
        self.tasks = tasks

    def set_current_task(self, task_name):
        self.current_task = task_name

    def get_task_logger(self, task_name):
        return TaskLogger(self, task_name)

    def report_event(self, **event):
        if "context" not in event:
            event["context"] = "app"

        elif event["context"] == "task":
            event["total_tasks"] = len(self.tasks)
            event["task_order"] = self.tasks.index(event["task"]) + 1

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
                stage=self.current_stage,
                sayn_version=self.sayn_version,
                project_git_commit=self.project_git_commit,
                project_name=self.project_name,
                ts=datetime.now(),
            )
        )

        for logger in self.loggers:
            logger.report_event(**event)

    def report_error(self, error):
        self.report_event(event="error", level="error", **error)

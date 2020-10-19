from pathlib import Path
from datetime import datetime
import subprocess

from .task_event_tracker import TaskEventTracker

from sayn import __version__ as sayn_version


class EventTracker:
    loggers = list()
    run_id = None
    current_stage = None
    current_stage_start_ts = None
    tasks = list()
    current_task = None
    current_task_n = 0
    sayn_version = sayn_version
    project_git_commit = None
    project_name = Path(".").absolute().name

    def __init__(self, run_id, loggers, run_arguments):
        self.run_id = run_id
        self.loggers = loggers
        try:
            self.project_git_commit = (
                subprocess.check_output(
                    ["git", "rev-parse", "HEAD"], stderr=subprocess.STDOUT
                )
                .split()[0]
                .decode("utf-8")
            )
        except:
            pass

        self.report_event(context="app", event="start_app", run_arguments=run_arguments)

    def register_logger(self, logger):
        self.loggers.append(logger)

    def start_stage(self, stage, **details):
        self.current_stage = stage
        self.current_stage_start_ts = datetime.now()
        self.report_event(context="app", event="start_stage", stage=stage, **details)

    def finish_current_stage(self, **details):
        duration = datetime.now() - self.current_stage_start_ts
        self.report_event(
            context="app",
            event="finish_stage",
            stage=self.current_stage,
            duration=duration,
            **details
        )
        self.current_stage = None
        self.current_stage_start_ts = None

    def set_tasks(self, tasks):
        self.tasks = tasks

    def get_task_tracker(self, task_name):
        if task_name in self.tasks:
            task_order = self.tasks.index(task_name) + 1
        else:
            task_order = None

        return TaskEventTracker(self, task_name, task_order)

    def report_event(self, **event):
        if "context" not in event:
            event["context"] = "app"

        elif event["context"] == "task":
            event["total_tasks"] = len(self.tasks)

        if "event" not in event:
            event["event"] = "unknown"

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

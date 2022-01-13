from pathlib import Path
from datetime import datetime
import subprocess
from typing import List, Optional

from .task_event_tracker import TaskEventTracker

from sayn import __version__ as sayn_version


class EventTracker:
    loggers = list()
    run_id = None
    current_stage = None
    current_stage_start_ts: Optional[datetime]
    tasks: List
    task_trackers: List[TaskEventTracker]
    current_task = None
    current_task_n = 0
    sayn_version = sayn_version
    project_git_commit = None
    project_name = Path(".").absolute().name

    def __init__(self, run_id):
        self.run_id = run_id
        self.tasks = list()
        try:
            self.project_git_commit = (
                subprocess.check_output(
                    ["git", "rev-parse", "HEAD"], stderr=subprocess.STDOUT
                )
                .split()[0]
                .decode("utf-8")
            )
        except:
            # If git is not available, we simply don't report the commit
            pass

    def register_logger(self, logger):
        self.loggers.append(logger)

    def remove_logger(self, logger_type):
        self.loggers = [l for l in self.loggers if not isinstance(l, logger_type)]

    def start_stage(self, stage, **details):
        self.current_stage = stage
        self.current_stage_start_ts = datetime.now()
        self.report_event(context="app", event="start_stage", stage=stage, **details)

    def finish_current_stage(self, **details):
        if self.current_stage_start_ts is None:
            self.current_stage_start_ts = datetime.now()

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

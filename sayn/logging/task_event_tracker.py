from datetime import datetime

from ..core.errors import Ok


class TaskEventTracker:
    # _logger = None
    # _task_name = None
    # _task_order = None
    # _steps = None
    # _current_step = None
    # _current_step_start_ts = None

    def __init__(self, logger, task_name, task_order):
        self._logger = logger
        self._task_name = task_name
        self._task_order = task_order
        self._steps = list()
        self._current_step = None
        self._current_step_start_ts = None

    def _report_event(self, event, **details):
        details["event"] = event
        details["context"] = "task"

        details["task"] = self._task_name
        details["task_order"] = self._task_order

        details["step"] = self._current_step
        details["step_order"] = (
            self._steps.index(self._current_step) + 1
            if self._current_step in self._steps
            else None
        )
        details["total_steps"] = len(self._steps)

        self._logger.report_event(**details)

    def set_run_steps(self, steps):
        self._report_event("set_run_steps", steps=steps)
        self._steps = steps

    def add_run_steps(self, steps):
        if isinstance(steps, str):
            steps = [steps]
        self._report_event("set_run_steps", steps=steps)
        self._steps.extend(steps)

    def start_step(self, step):
        self.finish_current_step()

        if step not in self._steps:
            self._steps.append(step)

        self._current_step = step
        self._current_step_start_ts = datetime.now()

        self._report_event("start_step")

    def finish_current_step(self, result=Ok()):
        if self._current_step is not None:
            if self._current_step_start_ts is not None:
                duration = datetime.now() - self._current_step_start_ts
            else:
                duration = None

            self._report_event(
                "finish_step",
                duration=duration,
                result=result,
            )
            self._current_step = None
            self._current_step_start_ts = None

    def debug(self, message, **details):
        self._report_event("message", level="debug", message=message, **details)

    def info(self, message, **details):
        self._report_event("message", level="info", message=message, **details)

    def warning(self, message, **details):
        self._report_event("message", level="warning", message=message, **details)

    def error(self, message, **details):
        self._report_event("message", level="error", message=message, **details)

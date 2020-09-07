from datetime import datetime

from ..core.errors import Ok


class TaskLogger:
    _logger = None
    _task_name = None
    steps = list()
    current_step = None
    current_step_start_ts = None

    def __init__(self, logger, task_name):
        self._logger = logger
        self.task_name = task_name

    def set_run_steps(self, steps):
        self._report_event(event="set_run_steps", steps=steps)
        self.steps = steps

    def start_step(self, step):
        self.finish_current_step()

        self.current_step = step
        self.current_step_start_ts = datetime.now()

        step_order = (
            self.steps.index(self.current_step)
            if self.current_step in self.steps
            else None
        )

        self._report_event(event="start_step", step=step, step_order=step_order)

    def finish_current_step(self, result=Ok()):
        if self.current_step is not None:
            step_order = (
                self.steps.index(self.current_step)
                if self.current_step in self.steps
                else None
            )
            duration = self.current_step_start_ts - datetime.now()

            self._report_event(
                event="finish_step",
                step=self.current_step,
                step_order=step_order,
                result=result,
                duration=duration,
            )
            self.current_step = None
            self.current_step_start_ts = None

    def debug(self, message, details=None):
        self._report_event(
            event="message",
            level="debug",
            step=self.step,
            message=message,
            details=details,
        )

    def info(self, message, details=None):
        self._report_event(
            event="message",
            level="info",
            step=self.step,
            message=message,
            details=details,
        )

    def warning(self, message, details=None):
        self._report_event(
            event="message",
            level="warning",
            step=self.step,
            message=message,
            details=details,
        )

    def error(self, message, details=None):
        self._report_event(
            event="message",
            level="error",
            step=self.step,
            message=message,
            details=details,
        )

    def _report_event(self, **event):
        event = {k: v for k, v in event.items() if v is not None}
        if "event" not in event and "message" in event:
            event["event"] = "message"
        elif "event" not in event:
            event["event"] = "unknown"
        event["context"] = "task"
        event["task"] = self.task_name
        if self.current_step is not None:
            event["step"] = self.current_step
        self._logger.report_event(**event)

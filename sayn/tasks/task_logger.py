class TaskLogger:
    _logger = None
    _task_name = None
    steps = list()
    current_step = None

    def __init__(self, logger, task_name):
        self._logger = logger
        self.task_name = task_name

    def set_run_steps(self, steps):
        self._report_event(event="set_run_steps", steps=steps)
        self.steps = steps

    def start_step(self, step):
        if len(self.steps) > 0 and step not in self.steps:
            raise ValueError(
                f"{step} not in defined steps. Use `self.logger.set_run_steps(list)` first."
            )
        elif self.current_step is not None:
            self._report_event(
                level="info",
                event="finish_step",
                step=self.current_step,
                step_order=self.steps.index(self.current_step)
                if len(self.steps) > 0
                else None,
            )

        self.current_step = step
        self._report_event(
            event="start_step",
            step=step,
            step_order=self.steps.index(self.current_step)
            if len(self.steps) > 0
            else None,
        )

    def finish_current_step(self, exception=None):
        if exception is not None:
            self._report_event(
                level="error",
                event="finish_step",
                step=self.current_step,
                step_order=self.steps.index(self.current_step)
                if len(self.steps) > 0
                else None,
                details=f"{exception}",
            )
        else:
            self._report_event(
                level="info",
                event="finish_step",
                step=self.current_step,
                step_order=self.steps.index(self.current_step)
                if len(self.steps) > 0
                else None,
            )
        self.current_step = None

    def debug(self, message, step=None, details=None):
        self._report_event(
            event="message", level="debug", message=message, step=step, details=details
        )

    def info(self, message, step=None, details=None):
        self._report_event(
            event="message", level="info", message=message, step=step, details=details
        )

    def warning(self, message, step=None, details=None):
        self._report_event(
            event="message",
            level="warning",
            message=message,
            step=step,
            details=details,
        )

    def error(self, message, step=None, details=None):
        self._report_event(
            event="message", level="error", message=message, step=step, details=details
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

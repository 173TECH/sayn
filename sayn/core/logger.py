class TaskLogger:
    _logger = None
    task_name = None
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
        self._logger.report_event(event)


class AppLogger:
    loggers = dict()
    stage = None
    run_id = None
    tasks = list()
    current_task = None

    def __init__(self, run_id):
        self.run_id = run_id
        self.stage = "setup"
        # self.loggers["console"] = ConsoleLogger(debug)

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
        # elif event['event'] == 'message':
        else:
            print(dict(run_id=self.run_id, stage=self.stage, **event))

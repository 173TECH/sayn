from .log_formatter import LogFormatter


class Logger:
    fmt = LogFormatter()
    current_indent = 0

    def unhandled(self, event, context, stage, details):
        self.print(self.fmt.unhandled(event, context, stage, details))

    def message(self, level, message, details):
        self.print(self.fmt.message(level, message, details))

    # App context

    def app_start(self, details):
        self.print(self.fmt.app_start(details))
        self.print()

    def app_finish(self, details):
        self.print(self.fmt.app_finish(details))

    def app_stage_start(self, stage, details):
        self.print(self.fmt.app_stage_start(stage, details))
        self.current_indent += 1

    def app_stage_finish(self, stage, details):
        self.current_indent -= 1
        self.print(self.fmt.app_stage_finish(stage, details))
        self.print()

    # Task context

    def task_stage_start(self, stage, task, task_order, total_tasks, details):
        self.print(
            self.fmt.task_stage_start(stage, task, task_order, total_tasks, details)
        )
        self.current_indent += 1

    def task_stage_finish(self, stage, task, task_order, total_tasks, details):
        self.current_indent -= 1
        self.print(
            self.fmt.task_stage_finish(stage, task, task_order, total_tasks, details)
        )
        if stage in ("run", "compile"):
            self.print()

    def task_set_steps(self, details):
        self.print(self.fmt.task_set_steps(details))

    def task_step_start(self, stage, task, step, step_order, total_steps, details):
        self.print(
            self.fmt.task_step_start(
                stage, task, step, step_order, total_steps, details
            )
        )
        self.current_indent += 1

    def task_step_finish(self, stage, task, step, step_order, total_steps, details):
        self.current_indent -= 1
        self.print(
            self.fmt.task_step_finish(
                stage, task, step, step_order, total_steps, details
            )
        )

    def report_event(self, context, event, stage, **details):
        if event == "message":
            self.message(details["level"], details["message"], details)

        elif context == "app":
            if event == "start_app":
                self.app_start(details)

            elif event == "finish_app":
                self.app_finish(details)

            elif event == "start_stage":
                self.app_stage_start(stage, details)

            elif event == "finish_stage":
                self.app_stage_finish(stage, details)

            else:
                self.unhandled(event, context, stage, details)

        elif context == "task":
            task = details["task"]
            task_order = details["task_order"]
            total_tasks = details["total_tasks"]

            if event == "set_run_steps":
                self.task_set_steps(details)

            elif event == "start_stage":
                self.task_stage_start(stage, task, task_order, total_tasks, details)

            elif event == "finish_stage":
                self.task_stage_finish(stage, task, task_order, total_tasks, details)

            elif event == "start_step":
                self.task_step_start(
                    stage,
                    task,
                    details["step"],
                    details["step_order"],
                    details["total_steps"],
                    details,
                )

            elif event == "finish_step":
                self.task_step_finish(
                    stage,
                    task,
                    details["step"],
                    details["step_order"],
                    details["total_steps"],
                    details,
                )

            else:
                self.unhandled(event, context, stage, details)

        else:
            self.unhandled(event, context, stage, details)

    def print(self, s=None):
        raise NotImplementedError("Logger requires print method")

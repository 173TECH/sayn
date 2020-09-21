from halo import Halo

from .logger import Logger
from .log_formatter import LogFormatter, human


class FancyLogger(Logger):
    fmt = LogFormatter(use_colour=False, use_icons=False, output_ts=True)
    cfmt = LogFormatter(use_colour=True, use_icons=False, output_ts=True)
    spinner = Halo(spinner="dots")

    stage = None
    task = None
    step = None
    task_order = None
    total_tasks = None
    step = None
    step_order = None
    total_steps = None
    task_text = None
    step_text = None
    task_persist_msgs = list()

    def message(self, level, message, details):
        fmsg = self.cfmt.message(level, message, details)
        self.task_persist_msgs.append(fmsg)
        txt = f"{self.task_text}: "
        if self.step_text is not None:
            txt += f"{self.step_text}: "
        txt += ". ".join(fmsg["message"])
        self.spinner.text = txt

    def task_stage_finish(self, stage, duration, result):
        self.spinner.text = (
            f"[{self.task_order}/{self.total_tasks}] {self.task} ({human(duration)})"
        )

        if result.is_ok:
            if stage == "setup":
                self.spinner.clear()
                self.spinner.text_color = None
            else:
                self.spinner.text_color = "green"
                self.spinner.succeed()
                self.spinner.text_color = None

        else:
            if result.error.code == "parent_errors":
                self.spinner.text_color = "yellow"
                self.spinner.warn()
            elif (
                result.error.code == "setup_error"
                and result.error.details["status"].value == "skipped"
            ):
                self.spinner.text_color = "yellow"
                self.spinner.warn()
            else:
                self.spinner.text_color = "red"
                self.spinner.fail()

            self.spinner.text_color = None

            self.current_indent += 1

            message = self.fmt.error_result(duration, result.error)

            if self.step is not None:
                step_progress = f"{self.step_order}/{self.total_steps}"

                if isinstance(message["message"], list):
                    message["message"].append(self.fmt.red(f"On step {self.step}:\n"))
                    self.current_indent += 1
                    self.print(message)
                    self.current_indent -= 1
                else:
                    message["message"] = (
                        self.fmt.red(f"On step {step_progress} {self.step}: ")
                        + message["message"]
                    )
                    self.print(message)

            else:
                self.print(message)

            self.current_indent -= 1

        self.task = None
        self.task_order = None
        self.task_text = None

        self.current_indent += 1
        for msg in self.task_persist_msgs:
            if msg["level"] in ("warning", "error"):
                self.print(msg)
        self.current_indent -= 1

        self.task_persist_msgs = list()

    def report_event(self, context, event, stage, **details):
        if event == "message":
            self.message(details["level"], details["message"], details)

        elif context == "app":
            if event == "start_app":
                self.app_start(details)
                print()

            elif event == "finish_app":
                self.app_finish(details)

            elif event == "start_stage":
                self.stage = stage
                self.app_stage_start(stage, details)

            elif event == "finish_stage":
                print()
                self.stage = None
                self.app_stage_finish(stage, details)
                print()

            else:
                self.unhandled(event, context, stage, details)

        elif context == "task":
            self.task = details["task"]
            self.task_order = details["task_order"]
            self.total_tasks = details["total_tasks"]
            ts = human(details["ts"])

            if event == "set_run_steps":
                # Less verbosity for this logger
                pass  # self.task_set_steps(details)

            elif event == "start_stage":
                self.task_text = f"[{self.task_order}/{self.total_tasks}] {self.task} (started at {ts})"
                self.spinner.text = self.task_text
                self.spinner.start()

            elif event == "finish_stage":
                self.task_stage_finish(stage, details["duration"], details["result"])

            elif event == "start_step":
                self.step = details["step"]
                self.step_order = details["step_order"]
                self.total_steps = details["total_steps"]
                self.step_text = (
                    f"Step [{self.step_order}/{self.total_steps}] {self.step}"
                )
                self.spinner.text = f"{self.task_text}: {self.step_text}"

            elif event == "finish_step":
                if details["result"].is_ok:
                    self.step = None
                    self.step_order = None

                self.step_text = None

            else:
                self.unhandled(event, context, stage, details)

        else:
            self.unhandled(event, context, stage, details)

    def print(self, s=None):
        if s is None:
            pass
        else:
            prefix = "  " * self.current_indent
            s = s["message"]
            if isinstance(s, str):
                s = [s]
            elif not isinstance(s, list):
                raise ValueError("error in logging print")

            print(f"{prefix}{s[0]}")
            for e in s[1:]:
                for l in e.split("\n"):
                    print(f"{prefix}  {l}")

from pathlib import Path
import logging

from colorama import init, Fore, Style
from halo import Halo
import humanize


init(autoreset=True)


class LogFormatter:
    level = "debug"
    unattended = False

    levels = ("success", "error", "warning", "info", "debug")
    fields = {
        "run_id": {"include": "prefix", "unattended": True},
        "project_name": {"include": "prefix", "unattended": True},
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

    def __init__(self, level, unattended):
        self.level = level
        self.unattended = unattended

    def get_lines(self, **event):
        if self.levels.index(event["level"]) > self.levels.index(self.level):
            return
        prefix = self.get_prefix(event)

        lines = list()
        if event["context"] == "app":
            lines = self.get_app_lines(**event)

        elif event["context"] == "task":
            lines = self.get_task_lines(**event)

        if self.level == "debug" and (lines is None or len(lines) == 0):
            # TODO testing logging code. Should never execute.
            yield f"{event}"
        else:
            for line in lines:
                if self.unattended:
                    yield f"{prefix}|" + line.replace("\n", "\\n")
                else:
                    yield f"{prefix}|{line}"

    def get_prefix(self, event):
        prefix_fields = list()
        for field, conf in self.fields.items():
            if conf.get("include") != "prefix":
                continue
            elif conf.get("unattended") and not self.unattended:
                continue
            if len(conf) > 0 and field in event:
                value = f'{conf.get("transform", lambda x: x)(event[field])}'
                prefix_fields.append(value)

        return f"{'|'.join(prefix_fields)}"

    def get_app_lines(self, event, context, stage, level, **kwargs):
        if event == "start_stage":
            return [f"{level.upper()}|Starting {stage} stage"]

        elif event == "finish_stage":
            return [f"{level.upper()}|Finished {stage} stage"]

        elif event == "execution_finished":
            prefix = f"{level.upper()}|"
            out = [
                ". ".join(
                    (
                        f"{prefix}Process finished",
                        f"Total tasks: {len(kwargs['succeeded']+kwargs['skipped']+kwargs['failed'])}",
                        f"Success: {len(kwargs['succeeded'])}",
                        f"Failed {len(kwargs['failed'])}",
                        f"Skipped {len(kwargs['skipped'])}.",
                    )
                )
            ]

            for status in ("succeeded", "failed", "skipped"):
                if len(kwargs[status]) > 0:
                    out.append(
                        (
                            f"The following tasks "
                            f"{'were ' if status == 'skipped' else ''}{status}: "
                            f"{', '.join(kwargs[status])}"
                        )
                    )

            out.append(
                f"{kwargs['command'].capitalize()} took {humanize.precisedelta(kwargs['duration'])}"
            )
            return out

    def get_task_lines(
        self, event, context, stage, level, task_order, total_tasks, **kwargs
    ):
        prefix = "|".join(
            (
                f"({task_order}/{total_tasks})",
                "INFO" if level == "success" else level.upper(),
                kwargs["task"],
            )
        )

        if event == "start_task":
            return [f"{prefix}|Starting."]

        elif event == "finish_task":
            out = list()
            if level == "error":
                out.append(
                    f"{prefix}|On step {kwargs['step']}: {kwargs['message']}"
                    if "step" in kwargs
                    else f"{prefix}|{kwargs['message']}"
                )

                if "details" in kwargs:
                    out.append(f"{prefix}|{kwargs['details']}")

            out.append(f"{prefix}|{humanize.precisedelta(kwargs['duration'])}")
            return out

        elif event == "cannot_run":
            return [f"{prefix}|SKIPPING"]

        elif event == "set_run_steps":
            return [f"{prefix}|Settings steps: {', '.join(kwargs['steps'])}"]

        elif event == "start_step":
            return [f"{prefix}|Starting step: {kwargs['step']}"]

        elif event == "finish_step":
            return [f"{prefix}|Finished step: {kwargs['step']}"]

        elif event == "message":
            return [f"{prefix}|{kwargs['message']}"]


class Logger:
    def report_event(self, **event):
        raise NotImplementedError()


class ConsoleDebugLogger(Logger):
    formatter = LogFormatter("debug", False)

    def report_event(self, **event):
        # TODO
        print(event)
        return
        style = self.get_colours(event)
        for line in self.formatter.get_lines(**event):
            self.print(line, style)

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


class ConsoleLogger(Logger):
    # TODO refactor log formatter so that it's more useful here
    # formatter = LogFormatter("info", False)
    spinner = None
    text = None

    def report_event(self, level, event, stage, **kwargs):
        if event == "start_stage" and stage != "summary":
            self.spinner = Halo(spinner="dots")
            self.spinner.start(stage)

        elif event == "finish_stage" and stage == "setup":
            if level == "error":
                self.spinner.fail(
                    "\n    ".join(
                        (
                            f"{Fore.RED}{stage.capitalize()} ({humanize.precisedelta(kwargs['duration'])}):",
                            f"{Fore.RED}Some tasks failed during setup: {', '.join(kwargs['task_statuses']['failed'])}",
                            "",
                        )
                    )
                )
            elif level == "warning":
                self.spinner.warn(
                    "\n    ".join(
                        (
                            f"{Fore.YELLOW}{stage.capitalize()} ({humanize.precisedelta(kwargs['duration'])}):",
                            f"{Fore.RED}Some tasks failed during setup: {', '.join(kwargs['task_statuses']['failed'])}",
                            f"{Fore.YELLOW}Some tasks will be skipped: {', '.join(kwargs['task_statuses']['skipped'])}",
                            "",
                        )
                    )
                )
            else:
                self.spinner.succeed(
                    f"{stage.capitalize()} ({humanize.precisedelta(kwargs['duration'])})"
                )

        elif event == "start_task":
            self.spinner.text = f"{stage.capitalize()}: {kwargs['task']}"

        elif event == "finish_task":
            if level == "error":
                self.spinner.fail(
                    f"{stage.capitalize()}: {kwargs['task']} ({humanize.precisedelta(kwargs['duration'])})"
                )
            elif level == "warning":
                self.spinner.warn(
                    f"{stage.capitalize()}: {kwargs['task']} ({humanize.precisedelta(kwargs['duration'])})"
                )
            else:
                self.spinner.succeed(
                    f"{stage.capitalize()}: {kwargs['task']} ({humanize.precisedelta(kwargs['duration'])})"
                )

        elif event == "execution_finished":
            out = [
                ". ".join(
                    (
                        ""
                        f"Process finished ({humanize.precisedelta(kwargs['duration'])})",
                        f"Total tasks: {len(kwargs['succeeded']+kwargs['skipped']+kwargs['failed'])}",
                        f"Success: {len(kwargs['succeeded'])}",
                        f"Failed {len(kwargs['failed'])}",
                        f"Skipped {len(kwargs['skipped'])}.",
                    )
                )
            ]

            for status in ("succeeded", "failed", "skipped"):
                if len(kwargs[status]) > 0:
                    out.append(
                        (
                            f"  The following tasks "
                            f"{'were ' if status == 'skipped' else ''}{status}: "
                            f"{', '.join(kwargs[status])}"
                        )
                    )

            colour = (
                Fore.GREEN
                if level == "success"
                else Fore.YELLOW
                if level == "warning"
                else Fore.RED
            )
            print()
            for line in out:
                print(f"{colour}{line}")


class FileLogger(Logger):
    formatter = LogFormatter("debug", True)
    logger = None

    def __init__(self, folder):
        formatter = logging.Formatter("%(message)s")

        log_file = Path(folder, "sayn.log")
        if not log_file.parent.exists():
            log_file.parent.mkdir(parents=True)

        handler = logging.FileHandler(log_file)
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(formatter)

        logger = logging.getLogger(__name__)
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)

        self.logger = logger

    def report_event(self, **event):
        # TODO
        return
        for line in self.formatter.get_lines(**event):
            self.logger.debug(line)

from pathlib import Path
import logging

from colorama import init, Fore, Style
from halo import Halo
from log_symbols import LogSymbols

from .misc import group_list, humanize

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
            "transform": lambda x: f"Took {humanize(x)}",
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
                f"{kwargs['command'].capitalize()} took {humanize(kwargs['duration'])}"
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

            out.append(f"{prefix}|{humanize(kwargs['duration'])}")
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

    def print_unhandled(self, event, context, stage, details):
        ignored = (
            "project_git_commit",
            "sayn_version",
            "project_name",
            "ts",
            "run_id",
            "task",
            "task_order",
            "total_tasks",
        )
        ctx = details["task"] if context == "task" else context
        print(
            f"Unhandled: {ctx}::{stage}::{event}:",
            {k: v for k, v in details.items() if k not in ignored},
        )

    # App context

    def print_app_start_stage(self, stage, details):
        if stage == "setup":
            print("Setting up...")
        elif stage in ("run", "compile"):
            print(
                Style.BRIGHT
                + f"Starting {stage} at {details['ts'].strftime('%H:%M:%S')}..."
            )
        else:
            self.print_unhandled("start_stage", "app", stage, details)

    def print_app_finish_stage(self, stage, details):
        tasks = group_list([(v.value, t) for t, v in details["tasks"].items()])
        failed = tasks.get("failed", list())
        succeeded = tasks.get("ready", list()) + tasks.get("succeeded", list())
        skipped = tasks.get("skipped", list())
        duration = details["duration"]

        if stage == "setup":
            print("Finished setup:")
            if len(failed) > 0:
                print(Fore.RED + f"  Failed tasks: {', '.join(failed)}")
            if len(skipped) > 0:
                print(Fore.YELLOW + f"  Tasks to skip: {', '.join(skipped)}")
            if len(succeeded) > 0:
                print("  Tasks to run: " + Style.BRIGHT + f"{', '.join(succeeded)}")

            print()

        elif stage in ("run", "compile"):
            if len(failed) > 0 or len(skipped) > 0:
                print(
                    Fore.RED
                    + f"There were some errors during {stage} (took {duration})"
                )
                if len(failed) > 0:
                    print(
                        Fore.RED
                        + f"    Failed tasks: {Fore.BRIGHT + ', '.join(failed)}"
                    )
                if len(skipped) > 0:
                    print(
                        Fore.YELLOW
                        + f"    Skipped tasks: {Fore.BRIGHT + ', '.join(skipped)}"
                    )
            else:
                print(
                    Fore.GREEN
                    + f"{stage.capitalize()} finished successfully in {humanize(duration)}"
                )
                print(
                    "    Tasks executed: "
                    + Style.BRIGHT
                    + f"{Style.BRIGHT + ', '.join(succeeded)}"
                )

        else:
            self.print_unhandled("finish_stage", "app", stage, details)

    # Task context

    def print_task_start_stage(self, stage, task, task_order, total_tasks, details):
        if stage == "setup":
            print(
                f"  [{task_order}/{total_tasks}] {Style.BRIGHT + task + Style.RESET_ALL}"
            )
        elif stage in ("run", "compile"):
            print(
                Style.BRIGHT
                + f"  [{task_order}/{total_tasks}] {task} "
                + Style.RESET_ALL
                + f"(started at {details['ts'].strftime('%H:%M')})"
            )
        else:
            self.print_unhandled("start_stage", "task", stage, details)

    def print_task_finish_stage(self, stage, task, task_order, total_tasks, details):
        if details["result"].is_ok:
            print(
                Fore.GREEN
                + f"   ✔ Finished {stage} for {Style.BRIGHT + task + Style.RESET_ALL} "
                + f"({humanize(details['duration'])})"
            )
        else:
            self.print_unhandled("finish_stage", "task", stage, details)

    def print_task_start_step(
        self, stage, task, step, step_order, total_steps, details
    ):
        if stage in ("run", "compile"):
            pass
            # print(
            #     f"    Starting {Style.BRIGHT + step + Style.RESET_ALL} "
            #     f"({step_order}/{total_steps}) at {details['ts'].strftime('%H:%M')}..."
            # )
        else:
            self.print_unhandled("start_step", "task", stage, details)

    def print_task_finish_step(
        self, stage, task, step, step_order, total_steps, details
    ):
        if details["result"].is_ok:
            print(
                Fore.GREEN
                + "    "
                + f"[{details['ts'].strftime('%H:%M')}] "
                + f"[{step_order}/{total_steps}] "
                + "✔ "
                + f"{Style.BRIGHT + step + Style.RESET_ALL} "
                + f"({humanize(details['duration'])})"
            )
        else:
            self.print_unhandled("finish_step", "task", stage, details)

    def report_event(self, context, event, stage, **details):
        if context == "app":
            if event == "start_app":
                debug = "(debug)" if details["run_arguments"]["debug"] else ""
                dt_range = (
                    f"{details['run_arguments']['start_dt']} to {details['run_arguments']['end_dt']}"
                    if not details["run_arguments"]["full_load"]
                    else "Full Load"
                )
                print(f"Starting sayn {debug}")
                print(f"   Run ID: {details['run_id']}")
                print(f"   Project: {details['project_name']}")
                print(f"   Sayn version: {details['sayn_version']}")
                if details["project_git_commit"] is not None:
                    print(f"   Git commit: {details['project_git_commit']}")
                print(f"   Period: {dt_range}")
                print(
                    f"   {'Profile: ' + (details['run_arguments'].get('profile') or 'Default')}"
                )
                print()

            elif event == "finish_app":
                errors = details["tasks"].get("failed", list()) + details["tasks"].get(
                    "skipped", list()
                )
                print()
                print(
                    Fore.RED
                    if len(errors) > 0
                    else Fore.GREEN
                    + f"Execution of SAYN took {humanize(details['duration'])}"
                )

            elif event == "start_stage":
                self.print_app_start_stage(stage, details)

            elif event == "finish_stage":
                self.print_app_finish_stage(stage, details)

            else:
                self.print_unhandled(event, context, stage, details)

        elif context == "task":
            task = details["task"]
            task_order = details["task_order"]
            total_tasks = details["total_tasks"]

            if event == "set_run_steps":
                print(f"    Run Steps: {Style.BRIGHT + ', '.join(details['steps'])}")

            elif event == "start_stage":
                self.print_task_start_stage(
                    stage, task, task_order, total_tasks, details
                )

            elif event == "finish_stage":
                self.print_task_finish_stage(
                    stage, task, task_order, total_tasks, details
                )

            elif event == "start_step":
                self.print_task_start_step(
                    stage,
                    task,
                    details["step"],
                    details["step_order"],
                    details["total_steps"],
                    details,
                )

            elif event == "finish_step":
                self.print_task_finish_step(
                    stage,
                    task,
                    details["step"],
                    details["step_order"],
                    details["total_steps"],
                    details,
                )

            else:
                self.print_unhandled(event, context, stage, details)

        else:
            self.print_unhandled(event, context, stage, details)

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
                            f"{Fore.RED}{stage.capitalize()} ({humanize(kwargs['duration'])}):",
                            f"{Fore.RED}Some tasks failed during setup: {', '.join(kwargs['task_statuses']['failed'])}",
                            "",
                        )
                    )
                )
            elif level == "warning":
                self.spinner.warn(
                    "\n    ".join(
                        (
                            f"{Fore.YELLOW}{stage.capitalize()} ({humanize(kwargs['duration'])}):",
                            f"{Fore.RED}Some tasks failed during setup: {', '.join(kwargs['task_statuses']['failed'])}",
                            f"{Fore.YELLOW}Some tasks will be skipped: {', '.join(kwargs['task_statuses']['skipped'])}",
                            "",
                        )
                    )
                )
            else:
                self.spinner.succeed(
                    f"{stage.capitalize()} ({humanize(kwargs['duration'])})"
                )

        elif event == "start_task":
            self.spinner.text = f"{stage.capitalize()}: {kwargs['task']}"

        elif event == "finish_task":
            if level == "error":
                self.spinner.fail(
                    f"{stage.capitalize()}: {kwargs['task']} ({humanize(kwargs['duration'])})"
                )
            elif level == "warning":
                self.spinner.warn(
                    f"{stage.capitalize()}: {kwargs['task']} ({humanize(kwargs['duration'])})"
                )
            else:
                self.spinner.succeed(
                    f"{stage.capitalize()}: {kwargs['task']} ({humanize(kwargs['duration'])})"
                )

        elif event == "execution_finished":
            out = [
                ". ".join(
                    (
                        "" f"Process finished ({humanize(kwargs['duration'])})",
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

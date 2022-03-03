from datetime import date, datetime, timedelta
import traceback

from colorama import init, Fore, Style

from ..utils.misc import group_list

init(autoreset=True)


def human(obj):
    if isinstance(obj, timedelta):
        secs = obj.total_seconds()
        mins = secs / 60.0
        hours = mins / 60.0

        if hours > 1.0:
            r = str(round(hours, 1))
            return f"{r if r[-1] != '0' else r[:-2]}h"

        if mins > 1.0:
            r = str(round(mins, 1))
            return f"{r if r[-1] != '0' else r[:-2]}m"

        elif secs > 1.0:
            r = str(round(secs, 1))
            return f"{r if r[-1] != '0' else r[:-2]}s"

        else:
            msecs = secs * 1000.0
            r = str(round(msecs, 1))
            return f"{r if r[-1] != '0' else r[:-2]}ms"
    elif isinstance(obj, datetime):
        return obj.strftime("%H:%M")
    elif isinstance(obj, str):
        return obj.replace("_", " ").title()
    else:
        return obj


class LogFormatter:
    use_colour = True
    use_icons = True
    output_ts = False

    def __init__(self, use_colour=True, use_icons=True, output_ts=False):
        self.use_colour = use_colour
        self.output_ts = output_ts
        self.use_icons = True

    # Styling methods

    def dim(self, s):
        if self.use_colour:
            return f"{Style.DIM}{s}{Style.NORMAL}"
        else:
            return s

    def bright(self, s):
        if self.use_colour:
            return f"{Style.BRIGHT}{s}{Style.NORMAL}"
        else:
            return s

    def red(self, s):
        if self.use_colour:
            return f"{Fore.RED}{s}{Fore.RESET}"
        else:
            return s

    def bright_red(self, s):
        if self.use_colour:
            return f"{Fore.RED}{Style.BRIGHT}{s}{Style.NORMAL}{Fore.RESET}"
        else:
            return s

    def yellow(self, s):
        if self.use_colour:
            return f"{Fore.YELLOW}{s}{Fore.RESET}"
        else:
            return s

    def bright_yellow(self, s):
        if self.use_colour:
            return f"{Fore.YELLOW}{Style.BRIGHT}{s}{Style.NORMAL}{Fore.RESET}"
        else:
            return s

    def green(self, s):
        if self.use_colour:
            return f"{Fore.GREEN}{s}{Fore.RESET}"
        else:
            return s

    def bright_green(self, s):
        if self.use_colour:
            return f"{Fore.GREEN}{Style.BRIGHT}{s}{Style.NORMAL}{Fore.RESET}"
        else:
            return s

    def indent(self, s, l):
        if self.use_colour:
            return " " * l + s
        else:
            return s

    def good(self, s):
        if self.use_colour and self.use_icons:
            return self.green(f"✔ {s}")
        else:
            return s

    def info(self, s):
        if self.use_colour and self.use_icons:
            return f"ℹ {s}"
        else:
            return s

    def warn(self, s):
        if self.use_colour and self.use_icons:
            return self.yellow(f"⚠ {s}")
        else:
            return s

    def bad(self, s):
        if self.use_colour and self.use_icons:
            return self.red(f"✖ {s}")
        else:
            return s

    def join(self, l, sep, f):
        if self.use_colour:
            return sep.join(f(s) for s in l)
        else:
            return sep.join(l)

    def blist(self, l):
        if self.use_colour:
            return self.join(l, ", ", self.bright)
        else:
            return ", ".join(l)

    # Event handling methods

    def unhandled(self, event, context, stage, details):
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
        return {
            "level": "error",
            "message": f"Unhandled: {ctx}::{stage}::{event}: "
            + {k: v for k, v in details.items() if k not in ignored}.__str__(),
        }

    def message(self, level, message, details):
        if not isinstance(message, list):
            message = [message]

        out = []
        ts = f"[{human(details['ts'])}]" if self.output_ts else ""

        for m in message:
            if level == "error":
                out.append(self.bad(f"{ts} {m}"))
            elif level == "warning":
                out.append(self.warn(f"{ts} {m}"))
            elif level == "debug":
                out.append(self.dim(f"{ts} {m}"))
            else:
                out.append(f"{ts} {m}")

        return {"level": level, "message": out}

    def error_result(self, duration, error):  # noqa: C901
        level = "error"
        message = self.bad(error.__str__())
        duration = human(duration)

        if "error_message" in error.details:
            message = self.bad(error.details["error_message"])

        elif error.kind == "exception":
            exc = error.details["exception"]
            message = [self.bad(f"Failed ({duration}) {exc}")]
            message.extend(
                [
                    self.red(l)
                    for it in traceback.format_exception(
                        exc, value=exc, tb=exc.__traceback__
                    )
                    for l in it.split("\n")
                ]
            )

        elif error.kind == "dag" and error.code == "cycle_error":
            level = "error"
            message = self.bad(
                f"A cycle was detected in the dag: {' > '.join(error.details['path'])}"
            )

        elif error.kind == "dag" and error.code == "missing_parents":
            level = "error"
            message = ["Some parents are missing from dag"] + [
                self.red(f"In task {self.bright(task)}: {', '.join(parents)}")
                for task, parents in error.details["missing"].items()
            ]

        elif error.kind == "dag" and error.code == "missing_sources":
            level = "error"
            message = self.bad(error.details["error_message"])

        elif error.kind == "dag" and error.code == "empty_dag":
            level = "error"
            message = self.bad("No tasks defined in this project")

        elif error.kind == "dag" and error.code == "empty_group":
            level = "error"
            message = self.bad(
                f'Group "{error.details["group"]}" contains no tasks. Please check the "file_name" property'
            )

        elif error.kind == "task_query" and error.code == "query_overlap":
            level = "error"
            message = self.bad(
                f"{error.details['overlap']} specified both as include and exclude"
            )

        elif error.kind == "task_query" and error.code == "incorrect_syntax":
            level = "error"
            message = self.bad(f'Incorrect filter syntax "{error.details["query"]}"')

        elif error.kind == "task_query" and error.code == "undefined_tag":
            level = "error"
            message = self.bad(
                f'Tag not found in the project: "{error.details["tag"]}"'
            )

        elif error.kind == "task_query" and error.code == "undefined_group":
            level = "error"
            message = self.bad(
                f'Group not found in the project: "{error.details["group"]}"'
            )

        elif error.kind == "task_query" and error.code == "undefined_task":
            level = "error"
            message = self.bad(
                f'Task not found in the project: "{error.details["task"]}"'
            )

        elif error.code == "wrong_credentials":
            level = "error"
            message = self.bad(
                f'Connections {self.bright(", ".join(error.details["credentials"]))} not defined in project.yaml'
            )

        elif error.code == "missing_credentials":
            level = "error"
            message = self.bad(
                f'Connections {self.bright(", ".join(error.details["credentials"]))} are required by project.yaml'
            )

        elif error.code == "missing_credential_type":
            level = "error"
            message = self.bad(
                f'Connections {self.bright(", ".join(error.details["credentials"]))} have no type'
            )

        elif error.kind == "tasks" and error.code == "task_fail":
            level = "error"
            message = self.bad(error.details["message"])

        elif error.code == "parent_errors":
            level = "warning"
            parents = ", ".join(
                [f"{p} ({s.value})" for p, s in error.details["failed_parents"].items()]
            )
            message = self.warn(
                f"Skipping due to ancestors errors: {parents} ({duration})"
            )

        elif error.code == "setup_error":
            if error.details["status"].value == "skipped":
                level = "warning"
                message = self.warn(f"Skipping due to parent errors ({duration})")
            else:
                level = "error"
                message = self.bad(f"Failed during setup ({duration})")

        elif error.code == "validation_error":
            level = "error"
            message = [self.bad(f"Validation errors found ({duration})")]
            message.extend(
                [
                    self.red(
                        f"  In {' > '.join([str(item) for item in e['loc']])}: {e['msg']}"
                    )
                    for e in error.details["errors"]
                ]
            )

        elif error.code == "sql_execution_error" and "message" in error.details:
            level = "error"
            message = self.bad(error.details["message"])

        elif error.kind == "database" and error.code == "exception":
            level = "error"
            message = self.bad(error.details["message"])

        elif error.kind == "database" and error.code == "sayn_error":
            level = "error"
            message = self.bad(error.details["error_message"])

        elif error.kind == "parsing" and "filename" in error.details:
            level = "error"
            if "error" in error.details:
                message = self.bad(
                    f"""Parsing error in file: {error.details['filename']}
                Details: {error.details['error']}
                   Line: {error.details['line']}"""
                )
            else:
                message = self.bad(f"File not found: {error.details['filename']}")

        elif error.kind == "task_type" and error.code == "invalid_task_type_error":
            level = "error"
            group = error.details["group"]
            task_type = error.details["type"]
            message = self.bad(
                f"""Task error in task group: {group}. Invalid task type: {task_type}.

            Current Valid Task Types:

            - autosql
            - sql
            - python
            - copy
            - dummy

            For more details please check SAYN documentation: https://173tech.github.io/sayn/tasks/overview/

            """
            )

        elif (
            error.kind == "python_loader"
            and error.code == "load_class_exception"
            and "exception" in error.details
        ):
            level = "error"
            message = self.bad(str(error.details["exception"]))

        elif (
            error.kind == "python_loader"
            and error.code == "missing_class"
            and "pyclass" in error.details
        ):
            level = "error"
            path = error.details["module_path"]
            if len(path) > 0:
                message = self.bad(
                    f"Error in file: {error.details['module_path']}.py. Missing Class: {error.details['pyclass']}."
                )
            else:
                message = self.bad(f"Invalid path: {error.details['pyclass']}")

        return {
            "level": level,
            "message": message,
        }

    # App context

    def app_start(self, details):
        debug = "(debug)" if details["debug"] else ""
        yesterday = date.today() - timedelta(days=1)
        if details["full_load"]:
            dt_range = "Full Load"
        elif (
            details["start_dt"] == details["end_dt"] and details["end_dt"] == yesterday
        ):
            dt_range = "Default"
        elif details["start_dt"] == details["end_dt"]:
            dt_range = f"{details['start_dt']}"
        else:
            dt_range = f"{details['start_dt']} to {details['end_dt']}"

        out = list()
        out.append(f"Starting sayn {debug}")
        out.append(f"Run ID: {details['run_id']}")
        out.append(f"Project: {details['project_name']}")
        out.append(f"Sayn version: {details['sayn_version']}")
        if details["project_git_commit"] is not None:
            out.append(f"Git commit: {details['project_git_commit']}")
        out.append(f"Period: {dt_range}")
        out.append(f"{'Profile: ' + (details.get('profile') or 'Default')}")

        return {"level": "info", "message": out}

    def app_finish(self, details):
        if "error" in details:
            return self.error_result(details["duration"], details["error"].error)
        else:
            errors = [
                t for t in list(details["tasks"].values()) if "FAILED" in str(t)
            ] + [t for t in list(details["tasks"].values()) if "SKIPPED" in str(t)]

            msg = f"Execution of SAYN took {human(details['duration'])}"
            if len(errors) > 0:
                return {"level": "error", "message": self.bad(msg)}
            else:
                return {"level": "info", "message": self.good(msg)}

    def app_stage_start(self, stage, details):
        if stage == "config":
            return {"level": "info", "message": "Configuring Project..."}
        elif stage == "setup":
            return {"level": "info", "message": "Setting up..."}
        elif stage in ("run", "compile", "test"):
            return {
                "level": "info",
                "message": self.bright(
                    f"Starting {stage} at {details['ts'].strftime('%H:%M')}..."
                ),
            }
        else:
            return self.unhandled("start_stage", "app", stage, details)

    def app_stage_finish(self, stage, details):
        tasks = group_list([(v.value, t) for t, v in details["tasks"].items()])
        failed = tasks.get("setup_failed", list()) + tasks.get("failed", list())
        succeeded = (
            tasks.get("ready", list())
            + tasks.get("ready_for_setup", list())
            + tasks.get("succeeded", list())
        )
        skipped = tasks.get("skipped", list())
        duration = human(details["duration"])
        totals_msg = (
            f"Total {'tasks' if details.get('test', False) is False else 'tests'}: {len(succeeded+failed+skipped)}. "
            f"Success: {len(succeeded)}. Failed {len(failed)}. Skipped {len(skipped)}."
        )
        if stage == "config":
            out = ["Finished project config:"]
            level = "info"
            if len(failed) > 0:
                out.append(self.bad(f"Tasks failed: {self.blist(failed)}"))
                level = "error"
            # if len(skipped) > 0:
            #     out.append(self.warn(f"Tasks to skip: {self.blist(skipped)}"))
            #     level = "error"
            if len(succeeded) > 0:
                out.append(
                    self.good(
                        f"{'Tasks' if details.get('test', False) is False else 'Tests'} found: {self.blist(succeeded)}"
                    )
                )
            return {"level": level, "message": out}

        elif stage == "setup":
            out = ["Finished setup:"]
            level = "info"
            type = "Tasks" if details.get("test", False) is False else "Tests"
            if len(failed) > 0:
                out.append(self.bad(f"{type} failed: {self.blist(failed)}"))
                level = "error"
            if len(skipped) > 0:
                out.append(self.warn(f"{type} to skip: {self.blist(skipped)}"))
                level = "error"
            if len(succeeded) > 0:
                out.append(self.good(f"{type} to run: {self.blist(succeeded)}"))
            return {"level": level, "message": out}

        elif stage in ("run", "compile", "test"):
            if len(failed) > 0 or len(skipped) > 0:
                out = [
                    self.red(
                        f"There were some errors during {stage} (took {duration})"
                    ),
                    self.red(totals_msg),
                ]
                if len(failed) > 0:
                    out.append(self.bad(f"Failed: {self.blist(failed)}"))
                if len(skipped) > 0:
                    out.append(self.warn(f"Skipped: {self.blist(skipped)}"))
                return {"level": "error", "message": out}
            else:
                return {
                    "level": "info",
                    "message": [
                        self.good(totals_msg),
                        self.good(
                            f"{stage.capitalize()} finished successfully in {duration}"
                        ),
                        f"Tasks executed: {self.blist(succeeded)}",
                    ],
                }

        else:
            return self.unhandled("finish_stage", "app", stage, details)

    # Task context

    def task_set_steps(self, details):
        return {
            "level": "info",
            "message": f"Run Steps: {self.blist(details['steps'])}",
        }

    def task_stage_start(self, stage, task, task_order, total_tasks, details):
        task_progress = f"[{task_order}/{total_tasks}]"
        ts = human(details["ts"])

        if stage == "config":
            return {"level": "info", "message": f"{self.bright(task)}"}
        elif stage == "setup":
            return {"level": "info", "message": f"{task_progress} {self.bright(task)}"}
        elif stage in ("run", "compile", "test"):
            return {
                "level": "info",
                "message": f"{self.bright(task_progress +' ' +task)} (started at {ts})",
            }
        else:
            return self.unhandled("start_stage", "task", stage, details)

    def task_stage_finish(self, stage, task, task_order, total_tasks, details):
        duration = human(details["duration"])

        if details.get("result") is None or details["result"].is_ok:
            return {
                "level": "info",
                "message": self.good(f"Took ({duration})"),
            }
        else:
            return self.error_result(details["duration"], details["result"].error)

    def task_step_start(self, stage, task, step, step_order, total_steps, details):
        task_progress = f"[{step_order}/{total_steps}]"
        ts = f"[{human(details['ts'])}]" if self.output_ts else ""

        if stage in ("run", "compile", "test"):
            return {
                "level": "info",
                "message": self.info(
                    f"{task_progress} {ts} Executing {self.bright(step)}"
                ),
            }
        else:
            return self.unhandled("start_step", "task", stage, details)

    def task_step_finish(self, stage, task, step, step_order, total_steps, details):
        task_progress = f"[{step_order}/{total_steps}]"
        ts = f"[{human(details['ts'])}]" if self.output_ts else ""
        duration = human(details["duration"])

        if details["result"].is_ok:
            return {
                "level": "info",
                "message": self.good(
                    f"{task_progress}" + f" {ts} {self.bright(step)} ({duration})"
                ),
            }
        else:
            return self.error_result(details["duration"], details["result"].error)

from colorama import init, Fore, Style
import humanize

from .logger import Logger

init(autoreset=True)


class ConsoleDebugLogger(Logger):
    level = "debug"
    levels = ("success", "error", "warning", "info", "debug")
    fields = {
        "run_id": {"include": "prefix"},
        "project_name": {"include": "prefix"},
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

    def __init__(self, level):
        self.level = level

    def report_event(self, **event):
        if self.levels.index(event["level"]) > self.levels.index(self.level):
            return
        style = self.get_colours(event)
        prefix = self.get_prefix(event)
        lines = self.get_lines(**event)
        # TODO testing
        if self.level == "debug" and len(lines) == 0:
            self.print(f"{event}", style)

        for line in lines:
            self.print(f"{prefix}|{line}", style)

    def get_prefix(self, event):
        prefix_fields = list()
        for field, conf in self.fields.items():
            if conf.get("include") != "prefix":
                continue
            if len(conf) > 0 and field in event:
                value = f'{conf.get("transform", lambda x: x)(event[field])}'
                prefix_fields.append(value)

        return f"{'|'.join(prefix_fields)}"

    def get_lines(self, event, context, stage, level, **kwargs):
        if context == "app":
            return self.get_app_lines(event, context, stage, level, **kwargs)

        elif context == "task":
            return self.get_task_lines(event, context, stage, level, **kwargs)

        return list()

    def get_app_lines(self, event, context, stage, level, **kwargs):
        if event == "start_stage":
            return [f"DEBUG|Starting {stage} stage"]

        elif event == "finish_stage":
            return [f"DEBUG|Finished {stage} stage"]

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
            return [f"{prefix}|Starting steps: {kwargs['step']}"]

        elif event == "finish_step":
            return [f"{prefix}|Finished step: {kwargs['step']}"]

        elif event == "message":
            return [f"{prefix}|{kwargs['message']}"]

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

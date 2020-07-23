from datetime import datetime

from .singleton import singleton


@singleton
class ConsoleLogger:
    def __init__(self, debug):
        self.debug = debug
        self.progress = None
        self.task_name = None
        self.stage_name = None

    def set_config(self, **kwargs):
        if "stage_name" in kwargs:
            self.stage_name = kwargs["stage_name"]
        if "task_name" in kwargs:
            self.task_name = kwargs["task_name"]
        if "progress" in kwargs:
            self.progress = kwargs["progress"]

    def logging_prefix(self, stage_name, sub_info=False, fst_line=False):
        task_name = self.task_name if self.task_name is not None else ""
        progress = self.progress if self.progress is not None else ""
        now = datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")

        if stage_name == "setup":
            prefix = now + " Setup|" + task_name
        if stage_name == "run":
            if fst_line:
                bar = ""
            else:
                bar = "|"
            prefix = now + " Run|" + progress + "|" + task_name + bar
        if stage_name == "summary":
            prefix = now + " Summary|"
        if sub_info:
            prefix = ""

        return prefix

    # process updates
    def print(self, text):
        print(self.logging_prefix(self.stage_name) + text)

    def print_debug(self, text):
        if self.debug:
            print(self.logging_prefix(self.stage_name) + text)

    def print_info(self, text):
        print(self.logging_prefix(self.stage_name) + text)

    def print_warning(self, text):
        print(
            "\u001b[93m" + self.logging_prefix(self.stage_name) + text + "\u001b[39;49m"
        )

    def print_error(self, text):
        print(
            "\u001b[91m" + self.logging_prefix(self.stage_name) + text + "\u001b[39;49m"
        )

    def print_success(self, text):
        print(
            "\u001b[92m" + self.logging_prefix(self.stage_name) + text + "\u001b[39;49m"
        )

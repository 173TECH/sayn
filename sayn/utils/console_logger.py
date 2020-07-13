from datetime import datetime
from halo import Halo
from .singleton import singleton


@singleton
class ConsoleLogger:
    def __init__(self, debug):
        self.spinner = Halo(spinner="dots")
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

    def spinner_prefix(self, stage_name, detail=False, fst_line=False):
        task_name = self.task_name if self.task_name is not None else ""
        progress = self.progress if self.progress is not None else ""

        if stage_name == "setup":
            prefix = self.start_ts + " Setup: " + task_name
        if stage_name == "run":
            if fst_line:
                col = ""
            else:
                col = ": "
            prefix = self.start_ts + " Run: " + progress + " " + task_name + col
        if stage_name == "summary":
            prefix = self.start_ts + " Summary: "
        if detail:
            prefix = ">    "  # we use that prefix as empty spaces or tabs does not seem to appear - to investigate

        return prefix

    def spinner_start(self):
        self.m_queue = []
        self.start_ts = datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
        self.spinner.start(self.spinner_prefix(self.stage_name, fst_line=True))

    def spinner_stop(self):
        for mess in self.m_queue:
            self.spinner.info(mess)
        self.spinner.stop()

    def spinner_stop_and_persist(self, text):
        self.spinner.text = self.spinner_prefix(self.stage_name) + text
        self.spinner.stop_and_persist()  # "ℹ"

    def spinner_set_text(self, text):
        self.spinner.text = self.spinner_prefix(self.stage_name) + text

    # process updates
    def spinner_debug(self, text):
        if self.debug:
            self.spinner_set_text(text)
            self.m_queue.append(
                (self.spinner_prefix(self.stage_name, detail=True) + text, "debug")
            )

    def spinner_info(self, text):
        self.spinner_set_text(text)
        self.m_queue.append(
            (self.spinner_prefix(self.stage_name, detail=True) + text, "info")
        )

    def spinner_info_new_line(self, text):
        self.spinner_start()
        self.spinner_stop_and_persist(text)

    def spinner_warn(self, text):
        self.spinner_set_text("Warning: " + text)
        self.m_queue.append(
            (self.spinner_prefix(self.stage_name, detail=True) + text, "warn")
        )

    def spinner_error(self, text):
        self.spinner_set_text("ERROR: " + text)
        self.m_queue.append(
            (self.spinner_prefix(self.stage_name, detail=True) + text, "error")
        )

    # summary for tasks - always printed in info mode below task summary line
    def spinner_sumup(self):
        for mess in self.m_queue:
            if mess[1] == "debug":
                self.spinner.info(mess[0])
            elif mess[1] == "info":
                self.spinner.info(mess[0])
            elif mess[1] == "warn":
                self.spinner.info(mess[0])
            elif mess[1] == "error":
                self.spinner.info(mess[0])
            else:
                pass

    # outcome statuses
    def spinner_succeed(self, text):
        self.spinner.text_color = "green"
        self.spinner.succeed(self.spinner_prefix(self.stage_name) + text)
        self.spinner.text_color = None
        # if self.stage_name == "run":
        self.spinner_sumup()

    def spinner_warning(self, text):
        self.spinner.text_color = "yellow"
        self.spinner.warn(self.spinner_prefix(self.stage_name) + text)
        self.spinner.text_color = None
        # if self.stage_name == "run":
        self.spinner_sumup()

    def spinner_fail(self, text):
        self.spinner.text_color = "red"
        self.spinner.fail(self.spinner_prefix(self.stage_name) + text)
        self.spinner.text_color = None
        # if self.stage_name == "run":
        self.spinner_sumup()

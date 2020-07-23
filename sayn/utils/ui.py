from pathlib import Path
import logging

from .singleton import singleton
from .file_logger import FileLogger
from .console_logger import ConsoleLogger


@singleton
class UI:
    def __init__(self, run_id=None, debug=False, log_file=Path("logs/sayn.log")):
        self.clog = ConsoleLogger(debug)
        self.flog = FileLogger(run_id=run_id, debug=debug, log_file=log_file)

    # UI API

    # set config
    def _set_config(self, **kwargs):
        self.clog.set_config(**kwargs)
        self.flog.set_config(**kwargs)

    # spinner start
    # def _start_spinner(self):
    #    self.clog.spinner_start()
    #    logging.info("Starting")

    # Process Logging
    def print(self, text):
        """
        Prints to the console
        Parameters:
        - text: the text to print.
        """
        self.clog.print(text)

    def debug(self, text):
        """
        Debug log to both the UI and the log file.
        Parameters:
        - text: the text to log.
        """
        self.clog.print_debug(text)
        logging.debug(text)

    def info(self, text):
        """
        Info log to both the UI and the log file.
        Parameters:
        - text: the text to log.
        """
        self.clog.print_info(text)
        logging.info(text)

    def warning(self, text):
        """
        Warning log to both the UI and the log file.
        Parameters:
        - text: the text to log.
        """
        self.clog.print_warning(text)
        logging.warning(text)

    def error(self, text):
        """
        Error log to both the UI and the log file.
        Parameters:
        - text: the text to log.
        """
        text = f"{text}"  # for errors
        self.clog.print_error(text)
        logging.error(text)

    ## Final status logging
    def _status_success(self, text):
        self.clog.print_success(text)
        logging.info("Success: " + text)

    def _status_warn(self, text):
        self.clog.print_warning(text)
        logging.warning("Warning: " + text)

    def _status_fail(self, text):
        self.clog.print_error(text)
        logging.error("Fail: " + text)

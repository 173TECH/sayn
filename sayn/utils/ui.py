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
    def _start_spinner(self):
        self.clog.spinner_start()
        logging.info("Starting")

    ## Progress logging
    def _debug(self, text, log_console=True, log_file=True):
        if log_console:
            self.clog.spinner_debug(text)
        if log_file:
            logging.debug(text)

    def _info(self, text, log_console=True, log_file=True, new_cl_line=False):
        if log_console:
            if new_cl_line:
                self.clog.spinner_info_new_line(text)
            else:
                self.clog.spinner_info(text)
        if log_file:
            logging.info(text)

    def _warn(self, text, log_console=True, log_file=True):
        if log_console:
            self.clog.spinner_warn(text)
        if log_file:
            logging.warning(text)

    def _error(self, text, log_console=True, log_file=True):
        if log_console:
            self.clog.spinner_error(text)
        if log_file:
            logging.error(text)

    ## Final status logging
    def _status_success(self, text, log_console=True, log_file=True):
        if log_console:
            self.clog.spinner_succeed(text)
        if log_file:
            logging.info("Success: " + text)

    def _status_warn(self, text, log_console=True, log_file=True):
        if log_console:
            self.clog.spinner_warning(text)
        if log_file:
            logging.warning("Warning: " + text)

    def _status_fail(self, text, log_console=True, log_file=True):
        if log_console:
            self.clog.spinner_fail(text)
        if log_file:
            logging.error("Fail: " + text)

    # Python SAYN API
    def debug(self, text, log_console=True, log_file=True):
        """
        Debug log to both the UI and the log file.
        Parameters:
        - text: the text of the log.
        - log_console: log to console if True (default).
        - log_file: log to file if True (default).
        """
        if log_console:
            self.clog.spinner_debug(text)
        if log_file:
            logging.debug(text)

    def info(self, text, log_console=True, log_file=True):
        """
        Info log to both the UI and the log file.
        Parameters:
        - text: the text of the log.
        - log_console: log to console if True (default).
        - log_file: log to file if True (default).
        """
        if log_console:
            self.clog.spinner_info(text)
        if log_file:
            logging.info(text)

    def warning(self, text, log_console=True, log_file=True):
        """
        Warning log to both the UI and the log file.
        Parameters:
        - text: the text of the log.
        - log_console: log to console if True (default).
        - log_file: log to file if True (default).
        """
        if log_console:
            self.clog.spinner_warn(text)
        if log_file:
            logging.warning(text)

    def error(self, text, log_console=True, log_file=True):
        """
        Error log to both the UI and the log file.
        Parameters:
        - text: the text of the log.
        - log_console: log to console if True (default).
        - log_file: log to file if True (default).
        """
        text = f"{text}"  # for errors
        if log_console:
            self.clog.spinner_error(text)
        if log_file:
            logging.error(text)

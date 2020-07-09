from datetime import datetime
from halo import Halo
from .singleton import singleton

@singleton
class UI:
    def __init__(self):
        self.spinner = Halo(spinner='dots')

    def set_config(self, task_name, tcounter=None, ntasks=None):
        self.task_name = task_name
        self.tcounter = tcounter
        self.ntasks = ntasks
        return self

    def spinner_prefix(self, is_start=False):
        now = datetime.now().strftime("[%Y-%m-%d %H:%M:%S]")
        #if setup there is no task counter
        if self.tcounter is not None and self.ntasks is not None:
            prefix = now + ' (' + str(self.tcounter) + '/' + str(self.ntasks) + ') ' + self.task_name
        else:
            prefix =  now + ' ' + self.task_name

        #amend : for non is_start for messages
        if is_start:
            return prefix
        else:
            return prefix + ': '

    def spinner_start(self):
        self.m_queue = []
        self.spinner.start(self.spinner_prefix(is_start=True))

    def spinner_stop(self):
        for mess in self.m_queue:
            self.spinner.info(mess)
        self.spinner.stop()

    def spinner_set_text(self, text):
        self.spinner.text = self.spinner_prefix() + text

    def spinner_info(self, text):
        self.spinner_set_text(text)
        self.m_queue.append((self.spinner_prefix() + text, 'info'))

    def spinner_warn(self, text):
        self.spinner.warn(self.spinner_prefix() + text)

    def spinner_sumup(self):
        for mess in self.m_queue:
            if mess[1] == 'info':
                self.spinner.info(mess[0])
            elif mess[1] == 'debug':
                self.spinner.info(mess[0])
            else:
                pass

    def spinner_succeed(self, text):
        self.spinner_sumup()
        self.spinner.succeed(self.spinner_prefix() + text)

    def spinner_fail(self, text):
        self.spinner_sumup()
        self.spinner.fail(self.spinner_prefix() + text)

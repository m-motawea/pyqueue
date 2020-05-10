from threading import Thread
from pyqueue.conclib.base import ConcBase
from pyqueue.conclib import CONC_STATUS
import time

class ThreadBackend(ConcBase):
    def __init__(self, function, calling_worker=None, fargs=[], fkwargs={}, daemon=True, timeout=None):
        super().__init__(function, calling_worker, fargs=fargs, fkwargs=fkwargs)
        self.t = None
        self.daemon = daemon
        self.timeout = timeout

    def run(self):
        def exec_function():
            try:
                result = self.function(*self.fargs, **self.fkwargs)
                self.result = result
                self._status = CONC_STATUS.success
            except Exception as e:
                self.exception = e
                self._status = CONC_STATUS.error

        self.t = Thread(name=self.uid, target=exec_function, daemon=self.daemon)
        self._status = CONC_STATUS.running
        self.t.start()

    def kill(self):
        self.t.kill()

    def join(self):
        self.t.join(timeout=self.timeout)

    @staticmethod
    def sleep(period):
        time.sleep(period)

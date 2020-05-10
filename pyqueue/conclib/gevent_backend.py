import gevent
from gevent import Greenlet
from pyqueue.conclib.base import ConcBase
from pyqueue.conclib import CONC_STATUS


class GeventBackend(ConcBase):
    def __init__(self, function, calling_worker=None, fargs=[], fkwargs={}, timeout=None):
        super().__init__(function, calling_worker, fargs=fargs, fkwargs=fkwargs)
        self.timeout = timeout
        self.t = None

    def run(self):
        def exec_function():
            try:
                result = self.function(*self.fargs, **self.fkwargs)
                self.result = result
                self._status = CONC_STATUS.success
            except Exception as e:
                self.exception = e
                self._status = CONC_STATUS.error

        self.t = Greenlet.spawn(exec_function)
        self._status = CONC_STATUS.running

    def kill(self):
        self.t.kill()

    def join(self):
        self.t.join(self.timeout)

    @property
    def status(self):
        if self.t.dead:
            self._status = CONC_STATUS.success
        return self._status

    def is_finished(self):
        return self.status == CONC_STATUS.success or self.status == CONC_STATUS.error

    @staticmethod
    def sleep(period):
        gevent.sleep(period)

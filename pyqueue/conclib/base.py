from pyqueue.conclib import CONC_STATUS
import uuid

class ConcBase:
    def __init__(self, function, calling_worker=None, fargs=[], fkwargs={}):
        self.function = function
        self.calling_worker = calling_worker
        self.fargs = fargs
        self.fkwargs = fkwargs
        self._status = None
        self.result = None
        self.exception = None
        self.uid = str(uuid.uuid4())


    def run(self):
        raise NotImplementedError

    def get_status(self):
        return self._status

    def get_result(self):
        return self.result

    def is_finished(self):
        return self._status == CONC_STATUS.success or self._status == CONC_STATUS.error

    def kill(self):
        raise NotImplementedError

    def join(self):
        raise NotImplementedError

    @staticmethod
    def sleep(period):
        raise NotImplementedError

from rediscluster import RedisCluster
from redis import Redis
import dill
from pyqueue.conclib.factory import ConcBackendFactory
import uuid
import time


class JOB_STATUS:
    not_started = "NOT_STARTED"
    running = "RUNNING"
    ready = "READY"
    finished = "FINISHED"
    failed = "FAILED"
    error = "ERROR"

class Job:
    def __init__(self, function, fargs=[], fkwargs={}, timeout=0, config={}):
        self.function = function
        self.fargs = fargs
        self.fkwargs = fkwargs
        self.timeout = timeout
        self.status = JOB_STATUS.not_started
        self.redis_config = config.get("REDIS", [{"host": "127.0.0.1", "port": "6379"}])
        self.redis_cluster = config.get("REDIS_CLUSTER", True)
        self.conc_backend = config.get("CONC_BACKEND", "THREAD")
        self.conc_instance = None
        self.uid = str(uuid.uuid4())
        self.exception = None
        self.result = None
        self.conn = None

    def publish(self, queue=None):
        def start():
            try:
                # define job dictionary
                job_dict = {
                    "function": dill.dumps(self.function),
                    "fargs": dill.dumps(self.fargs),
                    "fkwargs": dill.dumps(self.fkwargs),
                    "timeout": self.timeout,
                    "id": self.uid,
                    "status": JOB_STATUS.not_started
                }
                if self.redis_cluster:
                    self.conn = RedisCluster(startup_nodes=self.redis_config)
                else:
                    self.conn = Redis(**self.redis_config[0])
                # create job hash on redis
                self.conn.hmset("Job:" + str(self.uid), job_dict)
                # publish job id to redis JobsReady set
                self.conn.sadd("JobsReady", self.uid)
                # change job status to ready
                self.conn.hset("Job:" + str(self.uid), "status", JOB_STATUS.ready)
                self.status = JOB_STATUS.ready
                # periodically poll job status from job hash on Redis
                FINISHED = False
                while not FINISHED:
                    self.status = self.conn.hget("Job:" + self.uid, "status").decode()
                    if self.is_finished():
                        self.status = JOB_STATUS.finished
                        self.set_result(self.conn)
                        break
                    self.conc_instance.sleep(1)
            except Exception as e:
                self.status = JOB_STATUS.error
                self.execption = e

        if queue:
            raise NotImplementedError("queues are not supported yet")
        conc_class = ConcBackendFactory.GetBackend(self.conc_backend)
        self.conc_instance = conc_class(start, calling_worker=Job)
        self.conc_instance.run()

    def is_finished(self):
        return self.status == JOB_STATUS.finished or self.status == JOB_STATUS.failed or self.status == JOB_STATUS.error

    def set_result(self, conn):
        if self.status == JOB_STATUS.failed:
            # get exception from redis
            self.exception = dill.loads(self.conn.hget("Job:" + self.uid, "exception"))

        if self.status == JOB_STATUS.finished:
            # get result from redis
            self.result = dill.loads(self.conn.hget("Job:" + self.uid, "result"))

    def sleep(self, period):
        self.conc_instance.sleep(period)

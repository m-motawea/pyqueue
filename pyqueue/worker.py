from pyqueue.conclib.factory import ConcBackendFactory
from pyqueue.jobs import JOB_STATUS
from rediscluster import RedisCluster
from redis import Redis
import dill
import uuid
import datetime


class MaxConnectionRetriesExceededException(Exception):
    def __init__(self, max_number, host, port, exception, *args, **kwargs):
        super().__init__("Maximum number of connection retry exceeded {} to host: {} port: {} due to {}".format(
            max_number, host, port, repr(exception)), *args, **kwargs)


class WorkerStatus:
    ready = "READY"
    busy = "BUSY"
    starting = "STARTING"
    stopping = "STOPPING"


class Worker:
    def __init__(self, no_minions=1, config={}, queue=None):
        self.no_minions = no_minions
        self.redis_config = config.get("REDIS", [{"host": "127.0.0.1", "port": "6379"}])
        self.redis_cluster = config.get("REDIS_CLUSTER", True)
        self.conc_backend = config.get("CONC_BACKEND", "THREAD")
        self.minions = []
        self.uid = str(uuid.uuid4())
        self.conc_class = None
        self._conn = None
        self.max_reconnect = config.get("MAX_RECONNECT", 3)
        self.queue = queue
        self.status = WorkerStatus.starting
        self.time_started = datetime.datetime.now()
        self.last_heartbeat = self.time_started
        self.mapping = None
        self.keep_alive = config.get("WORKER_KEEPALIVE", 5)
        self.keep_alive_tb = None
        # monitoring params
        self.election_key = config.get("ELECTION_KEY", "MASTER_WORKER")
        self.sleep_period = config.get("SLEEP_PERIOD")
        self.is_master = False
        self.master_tb = None

    def start(self):
        self.conc_class = ConcBackendFactory.GetBackend(self.conc_backend)
        self._reconnect()
        self._update_worker_mapping()
        self._start_keepalive()
        self._register_worker()
        self._runloop()

    def _register_worker(self):
        self.conn.sadd("WorkersAvailable", self.uid)
        self.status = WorkerStatus.ready
        self._update_worker_mapping()

    def _update_worker_mapping(self):
        self.mapping = {
            "id": self.uid,
            "status": self.status,
            "no_minions": len(self.minions),
            "start_date": str(self.time_started),
            "last_heartbeat": str(self.last_heartbeat)
        }
        self.conn.hmset("Worker:" + self.uid, self.mapping)

    def _runloop(self):
        while True:
            if self.is_busy():
                self.conc_class.sleep(1)
            else:
                job = self._pull_job()
                self._exec_job(job)

    def _pull_job(self):
        job_id = None
        while not job_id:
            job_id = self.conn.spop("JobsReady")
            self.conc_class.sleep(1)
        job_id = job_id.decode()
        self.conn.sadd("JobsRunning", job_id)
        self.conn.hmset("Job:" + job_id, {"status": JOB_STATUS.running, "worker_id": self.uid})
        job = self.conn.hgetall("Job:" + job_id)
        return job


    def stop(self):
        pass

    def kill(self):
        exit(1)

    @property
    def conn(self):
        try:
            return self._conn
        except:
            self._reconnect()
        return self._conn

    def _reconnect(self):
        trials = self.max_reconnect
        while trials > 0:
            try:
                if self.redis_cluster:
                    self._conn = RedisCluster(startup_nodes=self.redis_config)
                    return
                else:
                    self._conn = Redis(**self.redis_config[0])
                    return
            except Exception as ex:
                trials -= 1
        if self.max_reconnect == 0:
            self._reconnect()
        else:
            raise MaxConnectionRetriesExceededException(
                self.max_reconnect,
                self.redis_config[0]["host"],
                self.redis_config[0]["port"],
                ex
            )

    def is_busy(self):
        pop_minions = []
        for i in range(0, len(self.minions)):
            if self.minions[i].is_finished():
                pop_minions.append(i)
        for i in pop_minions:
            self.minions.pop(i)
        if pop_minions:
            self._update_worker_mapping()

        if len(self.minions) >= self.no_minions:
            self.status = WorkerStatus.busy
            return True
        else:
            self.status = WorkerStatus.ready
            return False


    def _start_keepalive(self):
        def heartbeat():
            while True:
                self.last_heartbeat = datetime.datetime.now()
                self._update_worker_mapping()
                if self.is_master:
                    self.conn.expire(self.election_key, self.keep_alive + 3)
                    print("I'm master id: {}".format(self.uid))
                else:
                    print("I'm slave id: {}".format(self.uid))
                    self._elect()
                self.conc_class.sleep(2)

        self.keep_alive_tb = self.conc_class(heartbeat)
        self.keep_alive_tb.run()

    def _elect(self):
        print("starting election")
        self.is_master = self.conn.setnx(self.election_key, self.uid)
        if self.is_master:
            print("I became master {}".format(self.uid))
            self.conn.expire(self.election_key, self.keep_alive + 10)
            self.master_tb = self.conc_class(self._watch_workers)
            self.master_tb.run()
        return self.is_master

    def _watch_workers(self):
        while True:
            available_workers = self.conn.smembers("WorkersAvailable")
            dead_workers = []

            for worker in available_workers:
                worker_id = worker.decode()
                worker_last_heartbeat = self.conn.hget("Worker:" + worker_id, "last_heartbeat").decode()

                dead_time = datetime.datetime.strptime(worker_last_heartbeat,
                                                       "%Y-%m-%d %H:%M:%S.%f") + datetime.timedelta(
                    seconds=self.keep_alive + 1)

                if datetime.datetime.now() > dead_time:
                    dead_workers.append(worker_id)
                    self.conn.delete("Worker:" + worker_id)
                    self.conn.srem("WorkersAvailable", worker_id)
            if dead_workers:
                self._clear_workers(dead_workers)
            self.conc_class.sleep(5)

    def _clear_workers(self, dead_workers):
        running_jobs = self.conn.smembers("JobsRunning")
        for job_id in running_jobs:
            if self.conn.hget("Job:" + job_id.decode(), "worker") in dead_workers:
                self._reschedule(job_id.decode())


    def _reschedule(self, job_id):
        self.conn.sadd("JobsReady", job_id)
        self.conn.srem("JobsRunning", job_id)


    def _exec_job(self, job):
        func = dill.loads(job[b"function"])
        fargs = dill.loads(job.get(b"fargs"))
        fkwargs = dill.loads(job.get(b"fkwargs"))
        job_id = job[b"id"].decode()
        timeout = job[b"timeout"]
        def exec_function():
            try:
                result = func(*fargs, **fkwargs)
                self._publish_result(job_id, result)
            except Exception as e:
                self._publish_error(job_id, e)
        minion = self.conc_class(exec_function, timeout=timeout)
        minion.run()
        self.minions.append(minion)
        self._update_worker_mapping()

    def _publish_result(self, job_id, result):
        self.conn.hmset("Job:" + job_id, {"result": dill.dumps(result), "status": JOB_STATUS.finished})
        self.conn.srem("JobsRunning", job_id)
        self.conn.sadd("JobsFinished", job_id)

    def _publish_error(self, job_id, exception):
        self.conn.hmset("Job:" + job_id, {"exception": dill.dumps(exception), "status": JOB_STATUS.failed})
        self.conn.srem("JobsRunning", job_id)
        self.conn.sadd("JobsFinished", job_id)


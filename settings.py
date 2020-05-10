SETTINGS = {
    "CONC_BACKEND": "GEVENT",
    "MAX_RECONNECT": 5,  # maximum number of failed connection tries before the worker shutsdown
    "WORKER_KEEPALIVE": 10,  # time before update heartbeat field in worker hash on redis
    "ELECTION_KEY": "MASTER_WORKER",  # contains the id of master worker and expires after worker_keepalive
    "REDIS_CLUSTER": False,  # in case the host is not configured for cluster will use the first redis node only
    "REDIS": [  # at least one node for cluster discovery based on redis-py-cluster
        {
            "host": "127.0.0.1",
            "port": 6379
            # "username": "admin",
            # "password": "Admin@12#$"
        }
    ]
}

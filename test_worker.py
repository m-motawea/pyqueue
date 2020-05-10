from pyqueue.worker import Worker
from settings import SETTINGS

w = Worker(config=SETTINGS)
w.start()


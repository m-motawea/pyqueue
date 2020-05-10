from pyqueue.conclib.gevent_backend import GeventBackend
from pyqueue.conclib import CONC_STATUS
import requests
import time
import gevent

urls = ["https://www.google.com", "https://www.fb.com", "https://www.linkedin.com"]

def my_func(urls=[]):
    status = []
    #time.sleep(5)
    for url in urls:
        res = requests.get(url)
        status.append(res.status_code)
    return status

def func():
    print("done")

tbs = []
for i in range(0, 5):
    tb = GeventBackend(my_func, fargs=[urls])
    tb.run()
    tbs.append(tb)

FINISHED = False
while not FINISHED:
    FINISHED = True
    for tb in tbs:
        if not tb.is_finished():
            FINISHED = False
            print("tb: {} status is: {}. waiting...".format(tb.uid, tb.get_status()))
        else:
            print("tb: {} status is: {}.".format(tb.uid, tb.get_status()))
            if tb.get_status() == CONC_STATUS.success:
                print("\tresult: {}".format(tb.get_result()))
        GeventBackend.sleep(1)

print("tbs status is: {}".format(tb.get_status()))


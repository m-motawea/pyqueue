from pyqueue.conclib.thread_backend import ThreadBackend
from pyqueue.conclib import CONC_STATUS
import requests
import time

urls = ["https://www.google.com", "https://www.fb.com", "https://www.linkedin.com"]

def my_func(urls=[]):
    status = []
    print("executing")
    time.sleep(5)
    for url in urls:
        res = requests.get(url)
        status.append(res.status_code)
    return status

tbs = []
for i in range(0, 5):
    tb = ThreadBackend(my_func, fargs=[urls])
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
        ThreadBackend.sleep(1)

print("tbs status is: {}".format(tb.get_status()))


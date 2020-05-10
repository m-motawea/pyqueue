from pyqueue.jobs import JOB_STATUS, Job
import time
import requests
from settings import SETTINGS

urls = ["https://www.google.com", "https://www.fb.com", "https://www.linkedin.com"]

def my_func(urls=[]):
    import requests
    import time
    status = []
    print("executing")
    time.sleep(5)
    for url in urls:
        res = requests.get(url)
        status.append(res.status_code)
    return status

jobs = []
for i in range(0, 10):
    j = Job(my_func, [urls], config=SETTINGS)
    j.publish()
    jobs.append(j)


FINISHED = False
print("waiting_jobs")
while not FINISHED:
    FINISHED = True
    for j in jobs:
        if not j.is_finished():
            FINISHED = False
        else:
            print("job {} status: {}".format(j.uid, j.status))
            if j.status == JOB_STATUS.failed:
                print("execptions is: {}".format(str(j.exception)))

            if j.status == JOB_STATUS.finished:
                print("result is: {}".format(j.result))
    j.conc_instance.sleep(1)





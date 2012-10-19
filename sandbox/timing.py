
import time
import qb

start = time.time()
for i in xrange(10):
    jobs = qb.jobinfo(id=[26851, 26858, 26859, 26782])
    for job in jobs:
        print job['id'], job['status'], job.get('resultpackage')
    print
print time.time() - start
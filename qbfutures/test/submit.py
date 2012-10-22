import time
import os

from concurrent.futures import as_completed

from ..core import Executor



## SINGLE JOB

print 'SUBMIT'

future = Executor().submit('qbfutures.test.work:func', 'single')
print future
print future.result()
print


## MAP

print 'MAP'

for x in Executor(cpus=4).map('qbfutures.test.work:func', xrange(10)):
    print x

print

## BATCH

print 'BATCH'

with Executor().batch('Test Batch', cpus=4) as batch:
    first = batch.submit_ext('qbfutures.test.work:func', ['first'], name='first')
    second = batch.submit_ext('qbfutures.test.work:func', ['second'], name='second')
    time_f = batch.submit(time.time)
    cwd = batch.submit(os.getcwd)
    mapiter = batch.map(len, ['hello', 'world!'], name='map_test')

for future in as_completed(batch.futures):
    print future.job_id, future.work_id, future.result()

print 'map', list(mapiter)

print
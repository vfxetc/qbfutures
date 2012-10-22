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

for x in Executor().map('qbfutures.test.work:func', xrange(5)):
    print x

print

## BATCH

print 'BATCH'

with Executor().batch('Test Batch') as batch:
    first = batch.submit_ext('qbfutures.test.work:func', ['first'], name='first')
    second = batch.submit_ext('qbfutures.test.work:func', ['second'], name='second')
    time_f = batch.submit(time.time)
    cwd = batch.submit(os.getcwd)

for future in as_completed(batch.futures):
    print future.job_id, future.agenda_id, future.result()
    
print
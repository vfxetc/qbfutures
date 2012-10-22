import time
import os

from concurrent.futures import as_completed

from ..core import Executor
from .work import func

import qb


with Executor().batch('Test Batch') as batch:
    first = batch.submit_ext('qbfutures.test.work:func', ['first'], name='first')
    second = batch.submit_ext('qbfutures.test.work:func', ['second'], name='second')
    time_f = batch.submit(time.time)
    cwd = batch.submit(os.getcwd)

print 'as_completed'
for future in as_completed(batch.futures):
    print future.job_id, future.agenda_id, future.result()

exit()

future = Executor().submit_ext('qbfutures.test.work:maya_test', maya=2011, name='QBFutures Maya Test')
print future
print future.result()
print

future = Executor().submit('qbfutures.test.work:func', 'single')
print future
print future.result()
print

for x in Executor().map('qbfutures.test.work:func', xrange(40)):
    print x

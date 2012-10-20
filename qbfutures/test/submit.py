import time

from ..core import Executor
from .work import func

import qb


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

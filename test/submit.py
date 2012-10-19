import time

from ..core import Executor
from .work import func

import qb

for x in Executor().map('qbfutures.test.work:func', range(3)):
    print x

import time

from ..core import Executor
from .work import func

import qb

future = Executor().submit('qbfutures.test.work:func', 1, 2, 3)
print future
print future.id
print future.status()
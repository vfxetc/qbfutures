# Nobody puts us in the corner... except us.

# This is as clean of an execution environment as we can muster for apps like
# Maya which will execute arbitrary code in the __main__ module.

from qbfutures.worker import execute as _qbfutures_execute

_qbfutures_execute()

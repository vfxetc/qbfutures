from __future__ import absolute_import

import logging
import os

from concurrent.futures import as_completed


from .. import Executor


def dump_logging():
    import sys
    sys.path.append('/home/mboers/venv/lib/python2.6/site-packages')
    from logging_tree import printout
    printout()
    return True


if __name__ == '__main__':
    executor = Executor(
        name="Qube Logging Dump",
    )
    executor.submit('qbfutures.test.logging:dump_logging').result()
    


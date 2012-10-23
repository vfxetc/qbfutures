import time

from .. import core


def delay():
    time.sleep(1)


if __name__ == '__main__':

    print 'Weakref test'

    executor = core.Executor()
    executor.submit('qbfutures.test.weakref:delay')
    time.sleep(0.5)
    print 'number of items in poller', len(core._poller.futures)

from .. import Executor


def recurse():
    Executor().submit('qbfutures.test.recurse:recurse')


if __name__ == '__main__':
    recurse()

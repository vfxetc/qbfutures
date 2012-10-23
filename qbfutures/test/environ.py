import os
import sys
import pprint

def dump_environ():
    print 'os.environ'
    pprint.pprint(dict(os.environ))
    print
    
    print 'sys.path'
    pprint.pprint(list(sys.path))
    print


if __name__ == '__main__':
    
    from .. import submit_ext
    future = submit_ext('qbfutures.test.environ:dump_environ', name="QBFutures Python Environment Test", priority=8000)
    print 'python', future.job_id
    
    from ..maya import Executor
    future = Executor().submit_ext('qbfutures.test.environ:dump_environ', name='QBFutures Maya Environment Test', priority=8000)
    print 'maya', future.job_id

import threading
import os
import socket

from concurrent.futures import _base

import qb

class Future(_base.Future):
    
    def __init__(self):
        super(Future, self).__init__()
        self._id = None
    
    @property
    def id(self):
        return self._id
    
    def status(self):
        job = qb.jobinfo(id=self._id, fields=['status'])
        return job[0]['status']


class Executor(_base.Executor):
    
    def __init__(self):
        super(Executor, self).__init__()
    
    def submit(self, func, *args, **kwargs):
        
        job = {}
        job['prototype'] = 'qbfutures'
        job['name'] = 'QBFuture: %s' % (func,)
        
        job['cpus'] = 1
        job['env'] = dict(os.environ)
        
        job['agenda'] = qb.genframes('1')
        
        
        job['package'] = {
            'func': func,
            'args': args,
            'kwargs': kwargs,
        }
        
        submitted = qb.submit([job])
        assert len(submitted) == 1
        
        future = Future()
        future._id = submitted[0]['id']
        return future
    
        
        

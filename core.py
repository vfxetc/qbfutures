import threading
import os
import socket
import cPickle as pickle

from concurrent.futures import _base

import qb


FINISHED = set(('complete', 'pending', 'blocked'))


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


class _Poller(threading.Thread):
    
    # Singleton method.
    @classmethod
    def instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    _instance = None
    
    MIN_DELAY = 0.001
    MAX_DELAY = 2.0
    
    def __init__(self, two_stage_polling=False):
        super(_Poller, self).__init__()
        self.daemon = True
        
        self.two_stage_polling = two_stage_polling
        
        self.futures = {}
        self.delay = self.MAX_DELAY
        self.loop_event = threading.Event()
        self._started = False
    
    def add(self, future):
        self.futures[future.id] = future
        self.delay = self.MIN_DELAY
        self.loop_event.set()
        if not self._started:
            self._started = True
            self.start()
        
    def run(self):
        while True:
            
            self.delay = min(self.delay * 2, self.MAX_DELAY)
            #print 'WAIT %f' % self.delay
            self.loop_event.wait(self.delay)
            #print 'done wait'
            if self.loop_event.is_set():
                self.loop_event.clear()
            
            if not self.futures:
                self.delay = self.MAX_DELAY
                continue
            
            #print 'QUICK POLL'
            jobs = qb.jobinfo(id=self.futures.keys(), agenda=not self.two_stage_polling)
            #print 'done quick poll'
            
            if self.two_stage_polling:
                finished = [job['id'] for job in jobs if job['status'] in ('complete', 'failed')]
                if not finished:
                    continue
                #print 'LONG POLL'
                jobs = qb.jobinfo(id=finished, agenda=True)
                #print 'done long poll'
            
            for job in jobs:
                if job['status'] not in ('complete', 'failed'):
                    continue
                future = self.futures.pop(job['id'], None)
                if future is None:
                    continue
                agenda = job['agenda'][0]
                if agenda['status'] in ('complete', 'failed'):
                    
                    result = agenda['resultpackage'] or {}
                    if '__pickle__' in result:
                        result = pickle.loads(result['__pickle__'].decode('base64'))
                    # print 'result: %r' % result
                    
                    if 'result' in result:
                        future.set_result(result['result'])
                    elif 'exception' in result:
                        future.set_exception(result['exception'])
                    else:
                        future.set_exception(RuntimeError('no resultpackage'))


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
        job['package']['__pickle__'] = pickle.dumps(job['package']).encode('base64')
        
        submitted = qb.submit([job])
        assert len(submitted) == 1
        
        future = Future()
        future._id = submitted[0]['id']
        _Poller.instance().add(future)
        return future
    
        
        

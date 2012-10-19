import threading
import os
import socket
import cPickle as pickle
import time
import atexit

from concurrent.futures import _base

import qb

from . import utils


FINISHED = set(('complete', 'pending', 'blocked'))


class Future(_base.Future):
    
    def __init__(self, job_id, agenda_id):
        super(Future, self).__init__()
        self.job_id = job_id
        self.agenda_id = agenda_id
    
    def __repr__(self):
        res = super(Future, self).__repr__()
        if res.startswith('<Future '):
            res = ('<qbfutures.Future %d:%d ' % (self.job_id, self.agenda_id)) + res[8:]
        return res
    
    def status(self):
        job = qb.jobinfo(id=[self.job_id], agenda=True)
        return job[0]['agenda'][self.agenda_id]['status']


class _Poller(threading.Thread):
    
    MIN_DELAY = 0.001
    MAX_DELAY = 2.0
    
    def __init__(self, two_stage_polling=False):
        super(_Poller, self).__init__()
        self.daemon = True
        
        self.two_stage_polling = two_stage_polling
        
        self.futures = {}
        self.delay = self.MAX_DELAY
        self.loop_event = threading.Event()
        self.started = False
        self.running = True
    
    def add(self, future):
        self.futures[(future.job_id, future.agenda_id)] = future
    
    def trigger(self):
        self.delay = self.MIN_DELAY
        self.loop_event.set()
        if not self.started:
            self.started = True
            self.start()
    
    def shutdown(self):
        self.running = False
        self.futures.clear()
        self.loop_event.set()
        
    def run(self):
        while self.running:
            
            self.delay = min(self.delay * 2, self.MAX_DELAY)
            #print 'WAIT %f' % self.delay
            self.loop_event.wait(self.delay)
            #print 'done wait'
            if self.loop_event.is_set():
                self.loop_event.clear()
            
            if not self.futures:
                self.delay = self.MAX_DELAY
                continue
            
            # print 'QUICK POLL: %r' % self.futures.keys()
            jobs = qb.jobinfo(id=[f.job_id for f in self.futures.itervalues()], agenda=not self.two_stage_polling)
            # print 'done quick poll'
            
            if self.two_stage_polling:
                finished = [job['id'] for job in jobs if job['status'] in ('complete', 'failed')]
                if not finished:
                    continue
                #print 'LONG POLL'
                jobs = qb.jobinfo(id=finished, agenda=True)
                #print 'done long poll'
            
            for job in jobs:
                for agenda_i, agenda in enumerate(job['agenda']):
                    if agenda['status'] not in ('complete', 'failed'):
                        continue
                    
                    future = self.futures.pop((job['id'], agenda_i), None)
                    if future is None:
                        continue
                        
                    result = utils.unpack(agenda['resultpackage'])
                    # print 'RESULT: %r' % result
                    
                    if 'result' in result:
                        future.set_result(result['result'])
                    elif 'exception' in result:
                        future.set_exception(result['exception'])
                    else:
                        future.set_exception(RuntimeError('invalid resultpackage'))


_poller = _Poller()
atexit.register(_poller.shutdown)


class Executor(_base.Executor):
    
    def __init__(self):
        super(Executor, self).__init__()
    
    def _base_job(self, func, name=None, cpus=1, **kwargs):
        job = {}
        job['prototype'] = 'qbfutures'
        job['name'] = name or 'Python: %s' % (func,)
        job['cpus'] = cpus or 1
        job['env'] = {'QBFUTURES_PATH': os.path.abspath(os.path.join(__file__, '..', '..'))}
        job['agenda'] = []
        return job
        
    def _submit(self, job):
        submitted = qb.submit([job])
        assert len(submitted) == 1
        futures = []
        poller = _poller
        for i, agenda in enumerate(job['agenda']):
            future = Future(submitted[0]['id'], i)
            poller.add(future)
            futures.append(future)
        poller.trigger()
        return futures
        
    def submit(self, func, *args, **kwargs):
        return self.submit_ext(func, args, kwargs)
        
    def submit_ext(self, func, args, kwargs, **ext_kwargs):
        
        job = self._base_job(func, **ext_kwargs)
        
        package = {'func': func}
        if args:
            package['args'] = args
        if kwargs:
            package['kwargs'] = kwargs
        if 'interpreter' in ext_kwargs:
            package['interpreter'] = ext_kwargs['interpreter']
        
        agenda = qb.Work()
        agenda['name'] = '1'
        agenda['package'] = utils.pack(package)
        
        job['agenda'] = [agenda]
        
        return self._submit(job)[0]
    
    def map(self, func, *iterables, **kwargs):
        
        timeout = kwargs.get('timeout')
        if timeout is not None:
            end_time = timeout + time.time()
        
        job = self._base_job(func, **kwargs)
        
        for i, args in enumerate(zip(*iterables)):
            agenda = qb.Work()
            agenda['name'] = str(i + 1)
            agenda['package'] = utils.pack({
                'func': func,
                'args': args,
            })
            job['agenda'].append(agenda)
        
        futures = self._submit(job)
        
        try:
            for future in futures:
                if timeout is None:
                    yield future.result()
                else:
                    yield future.result(end_time - time.time())
        finally:
            for future in futures:
                future.cancel()


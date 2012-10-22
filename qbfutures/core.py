import threading
import os
import socket
import cPickle as pickle
import time
import atexit
import Queue as queue

from concurrent.futures import _base

import qb

from . import utils


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
        job = qb.jobinfo(id=[self.job_id])
        return job[0]['agendastatus']


class _Poller(threading.Thread):
    
    MIN_DELAY = 0.1
    MAX_DELAY = 2.0
    
    def __init__(self, two_stage_polling=False):
        super(_Poller, self).__init__()
        self.daemon = True
        
        self.two_stage_polling = two_stage_polling
        
        self.futures = {}
        self.new_futures = queue.Queue()
        self.delay = self.MAX_DELAY
        self.loop_event = threading.Event()
        self.started = False
        self.running = True
    
    def add(self, future):
        self.new_futures.put(future)
    
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
            
            # Wait for a timer, or for someone to trigger us. We want to wait
            # slightly longer each time, but given the nature of qube a 2x
            # increase in delay leads us to log waits too quickly.
            self.delay = min(self.delay * 1.15, self.MAX_DELAY)
            # print 'WAITING FOR', self.delay
            self.loop_event.wait(self.delay)
            if self.loop_event.is_set():
                self.loop_event.clear()
            
            # Get all the new futures. If we don't have any and there aren't any
            # in the queue, then wait on the queue for something to show up.
            queue_emptied = False
            while not queue_emptied or not self.futures:
                try:
                    # Block while we don't have any futures.
                    future = self.new_futures.get(not self.futures)
                
                except queue.Empty:
                    queue_emptied = True
                
                else:
                    self.futures[(future.job_id, future.agenda_id)] = future
                    
                    # We did just get something from the queue, so it
                    # potentially has more. Keep emptying it without fear or
                    # re-entering the loop since futures will be non-empty.
                    queue_emptied = True
            
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
                    
                    # Back up to full speed.
                    self.delay = self.MIN_DELAY
                    
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


class BatchFuture(Future):
    
    def __init__(self, agenda):
        super(BatchFuture, self).__init__(0, 0)
        self.agenda = agenda


class Batch(object):

    def __init__(self, executor, job):
        self._executor = executor
        self._job = job
        self.futures = []
        self.pending_futures = []
    
    def submit(self, func, *args, **kwargs):
        return self.submit_ext(func, args, kwargs)
    
    def submit_ext(self, func, args=None, kwargs=None, **ext_kwargs):
        agenda = qb.Work()
        agenda['name'] = ext_kwargs.get('name',
            '%d: %s' % (len(self.futures) + 1, utils.get_callable_name(func))
        )
        agenda['package'] = utils.pack(self._executor._base_package(func, args, kwargs, ext_kwargs))
        future = BatchFuture(agenda)
        self.pending_futures.append(future)
        return future
    
    def __enter__(self):
        return self
    
    def __exit__(self, *exc_info):
        if not exc_info[0]:
            self.apply()
    
    def apply(self):
        self._job['agenda'] = [future.agenda for future in self.pending_futures]
        submitted = qb.submit([self._job])
        assert len(submitted) == 1
        for i, future in enumerate(self.pending_futures):
            future.job_id = submitted[0]['id']
            future.agenda_id = i
            _poller.add(future)
            self.futures.append(future)
        self.pending_futures = []
        _poller.trigger()
        return self.futures


class Executor(_base.Executor):
    
    def __init__(self, **kwargs):
        super(Executor, self).__init__()
    
    def batch(self, name=None, **kwargs):
        kwargs['name'] = name or 'Python Batch'
        job = self._base_job(None, **kwargs)
        return Batch(self, job)
        
    def _base_job(self, func, name=None, cpus=1, env=None, **kwargs):
        job = {}
        job['prototype'] = 'qbfutures'
        job['name'] = name or 'Python: %s' % utils.get_callable_name(func)
        job['cpus'] = cpus or 1
        job['env'] = dict(env or {})
        job['env']['QBFUTURES_PATH'] = os.path.abspath(os.path.join(__file__, '..', '..'))
        if 'KS_DEV_ARGS' in os.environ:
            job['env']['KS_DEV_ARGS'] = os.environ['KS_DEV_ARGS']
        job['agenda'] = []
        job['package'] = {}
        return job
    
    def _base_package(self, func, args=None, kwargs=None, ext_kwargs=None):
        package = {
            'callable': func,
            'args': args or (),
            'kwargs': dict(kwargs or {}),
        }
        ext_kwargs = ext_kwargs or {}
        if 'interpreter' in ext_kwargs:
            package['interpreter'] = ext_kwargs['interpreter']
        return package
    
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
        
    def submit_ext(self, func, args=None, kwargs=None, **ext_kwargs):
        job = self._base_job(func, **ext_kwargs)
        agenda = qb.Work()
        agenda['name'] = '1'
        agenda['package'] = utils.pack(self._base_package(func, args, kwargs, ext_kwargs))
        job['agenda'] = [agenda]
        return self._submit(job)[0]
    
    def map(self, func, *iterables, **kwargs):
        
        job = self._base_job(func, **kwargs)
        
        for i, args in enumerate(zip(*iterables)):
            agenda = qb.Work()
            agenda['name'] = str(i + 1)
            agenda['package'] = utils.pack(self._base_package(func, args, None, kwargs))
            job['agenda'].append(agenda)
        
        futures = self._submit(job)
        return self._map_iter(futures, kwargs.get('timeout'))
    
    def _map_iter(self, futures, timeout):
        if timeout is not None:
            end_time = timeout + time.time()
        try:
            for future in futures:
                if timeout is None:
                    yield future.result()
                else:
                    yield future.result(end_time - time.time())
        finally:
            for future in futures:
                future.cancel()




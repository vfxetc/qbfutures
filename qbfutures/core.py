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
from . import poller


# Create one poller.
_poller = poller.Poller()
del poller


class Future(_base.Future):
    
    def __init__(self, job_id, work_id):
        super(Future, self).__init__()
        self.job_id = job_id
        self.work_id = work_id
    
    def __repr__(self):
        res = super(Future, self).__repr__()
        if res.startswith('<Future '):
            res = ('<qbfutures.Future %d:%d ' % (self.job_id, self.work_id)) + res[8:]
        return res
    
    def status(self):
        job = qb.jobinfo(id=[self.job_id])
        return job[0]['agendastatus']


class BatchFuture(Future):
    
    def __init__(self, work):
        super(BatchFuture, self).__init__(0, 0)
        self.work = work


class Batch(object):

    def __init__(self, executor, job):
        self.executor = executor
        self.job = job
        self.futures = []
    
    def submit(self, func, *args, **kwargs):
        return self.submit_ext(func, args, kwargs)
    
    def submit_ext(self, func, args=None, kwargs=None, **extra):
        work = qb.Work()
        work['name'] = extra.get('name',
            '%d: %s' % (len(self.futures) + 1, utils.get_func_name(func))
        )
        work['package'] = utils.pack(self.executor._base_work_package(func, args, kwargs, extra))
        future = BatchFuture(work)
        self.futures.append(future)
        return future
    
    def map(self, func, *iterables, **extra):
        futures = []
        for i, args in enumerate(zip(*iterables)):
            
            work = qb.Work()
            package = self.executor._base_work_package(func, args, None, extra)
            work['name'] = package.pop('name', str(i + 1))
            work['package'] = utils.pack(package)
            
            future = BatchFuture(work)
            futures.append(future)
            self.futures.append(future)
        
        return self.executor._map_iter(futures, extra.get('timeout'))
    
    def __enter__(self):
        return self
    
    def __exit__(self, *exc_info):
        if not exc_info[0]:
            self.commit()
    
    def commit(self):
        self.job['agenda'] = [future.work for future in self.futures]
        submitted = qb.submit([self.job])
        assert len(submitted) == 1
        for i, future in enumerate(self.futures):
            future.job_id = submitted[0]['id']
            future.work_id = i
            _poller.add(future)
        _poller.trigger()
        return self.futures


class Executor(_base.Executor):
    
    def __init__(self, **kwargs):
        super(Executor, self).__init__()
        self.defaults = kwargs
    
    def batch(self, name='QBFutures Batch', **kwargs):
        kwargs['name'] = name
        job = self._base_job(None, **kwargs)
        return Batch(self, job)
        
    def _base_job(self, func, **kwargs):
        
        job = dict(self.defaults)
        job.update(kwargs)
        
        job.setdefault('prototype', 'qbfutures')
        job.setdefault('name', 'QBFutures: %s' % utils.get_func_name(func))
        
        # Make sure this is a clean copy.
        job['env'] = dict(job.get('env') or {})
        job['env']['QBFUTURES_PATH'] = os.path.abspath(os.path.join(__file__, '..', '..'))
        if 'KS_DEV_ARGS' in os.environ:
            job['env']['KS_DEV_ARGS'] = os.environ['KS_DEV_ARGS']
        
        job['agenda'] = []
        job['package'] = {}
        
        return job
    
    def _base_work_package(self, func, args=None, kwargs=None, extra=None):
        
        package = {
            'func': func,
            'args': args or (),
            'kwargs': dict(kwargs or {}),
        }
        
        extra = extra or {}
        for attr in ('interperter', 'name'):
            if attr in extra:
                package[attr] = extra[attr]
        
        return package
    
    def _submit(self, job):
        
        job_id = qb.submit([job])[0]['id']
        
        futures = []
        for work_id, work in enumerate(job['agenda']):
            future = Future(job_id, work_id)
            futures.append(future)
            _poller.add(future)
        
        _poller.trigger()
        return futures
        
    def submit(self, func, *args, **kwargs):
        return self.submit_ext(func, args, kwargs)
        
    def submit_ext(self, func, args=None, kwargs=None, **extra):
        job = self._base_job(func, **extra)
        work = qb.Work()
        package = self._base_work_package(func, args, kwargs, extra)
        work['name'] = package.pop('name', '1')
        work['package'] = utils.pack(package)
        job['agenda'] = [work]
        return self._submit(job)[0]
    
    def map(self, func, *iterables, **extra):
        
        job = self._base_job(func, **extra)
        
        for i, args in enumerate(zip(*iterables)):
            work = qb.Work()
            package = self._base_work_package(func, args, None, extra)
            work['name'] = package.pop('name', str(i + 1))
            work['package'] = utils.pack(package)
            job['agenda'].append(work)
        
        futures = self._submit(job)
        return self._map_iter(futures, extra.get('timeout'))
    
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




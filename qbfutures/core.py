import atexit
import cPickle as pickle
import itertools
import os
import Queue as queue
import socket
import threading
import time
import sys

try:
    from concurrent.futures import _base
except ImportError:
    print 'COULD NOT FIND CONCURRENT.FUTURES'
    for x in sorted(os.environ.iteritems()):
        print '%s = %s' % x
    print
    print '\n'.join(sys.path)
    print
    raise
    
import qb

from . import utils
from . import poller


# Create one poller.
_poller = poller.Poller()
del poller


class Future(_base.Future):
    
    """A Future representing a unit of work on Qube."""
    
    def __init__(self, job_id, work_id):
        super(Future, self).__init__()
        
        #: The Qube job ID.
        self.job_id = job_id
        
        #: The index of this work item into the job's agenda.
        self.work_id = work_id
    
    def __repr__(self):
        res = super(Future, self).__repr__()
        if res.startswith('<Future '):
            res = ('<qbfutures.Future %d:%d ' % (self.job_id, self.work_id)) + res[8:]
        return res
    
    def status(self):
        """Get the current status for this particular work item."""
        job = qb.jobinfo(id=[self.job_id])
        return job[0]['agenda'][self.work_id]['status']


class BatchFuture(Future):
    
    def __init__(self, work):
        super(BatchFuture, self).__init__(0, 0)
        self.work = work


class Batch(object):
    
    """Pseudo-executor that submits callables into a single Qube job.
    
    Be careful not to use any of the resulting futures until the jobs have been
    submitted, either by using the ``Batch`` as a context manager, or calling
    :func:`~qbfutures.core.Batch.commit`.
    
    """
    
    def __init__(self, executor, job):
        self.executor = executor
        self.job = job
        self.futures = []
    
    def submit(self, func, *args, **kwargs):
        """Same as :func:`Executor.submit <qbfutures.Executor.submit>`"""
        return self.submit_ext(func, args, kwargs)
    
    def submit_ext(self, func, args=None, kwargs=None, **extra):
        """Same as :meth:`Executor.submit_ext <qbfutures.Executor.submit_ext>`,
        except extra keyword arguments are passed to the ``qb.Work``.
        
        """
        work = qb.Work()
        work['name'] = extra.get('name',
            '%d: %s' % (len(self.futures) + 1, utils.get_func_name(func))
        )
        work['package'] = utils.pack(self.executor._base_work_package(func, args, kwargs, extra))
        future = BatchFuture(work)
        self.futures.append(future)
        return future
    
    def map(self, func, *iterables, **extra):
        """Same as :meth:`Executor.map <qbfutures.Executor.map>`,
        except extra keyword arguments are passed to the ``qb.Work``.
        
        """
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
        """Perform the actual job submittion. Called automatically if used as
        a context manager."""
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
    
    """An object which provides methods to execute functions asynchonously on Qube.
    
    Any keyword arguments passed to the constructor are used as a template for
    every job submitted to Qube.
    
    """
    
    environ_passthroughs = ['KS_DEV_ARGS']
    
    def __init__(self, **kwargs):
        super(Executor, self).__init__()
        self.defaults = kwargs
        
    def _base_job(self, func, **kwargs):
        
        job = dict(self.defaults)
        job.update(kwargs)
        
        job.setdefault('prototype', 'qbfutures')
        job.setdefault('name', 'QBFutures: %s' % utils.get_func_name(func))
        job['name'] = str(job['name'])
        
        # Make sure this is a clean dict.
        job['env'] = dict(job.get('env') or {})
        
        # For bootstrapping development of this package.
        job['env']['QBFUTURES_DIR'] = os.path.abspath(os.path.join(__file__, '..', '..'))
        
        # Passthrough select environment variables.
        for name in itertools.chain(self.environ_passthroughs, ('QBFUTURES_RECURSION_LIMIT', )):
            if name in os.environ:
                job['env'][name] = os.environ[name]
        
        # Make sure the recursion depth isn't too high.
        depth = int(os.environ.get('QBLVL', 0))
        limit = int(os.environ.get('QBFUTURES_RECURSION_LIMIT', 4))
        if depth > limit:
            raise RuntimeError('Qube recursion reached limit of %s' % limit)
        job['env']['QBLVL'] = str(depth + 1)
        
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
        for attr in ('interpreter', 'name'):
            if attr in self.defaults:
                package[attr] = self.defaults[attr]
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
        """Schedules the given callable to be executed as ``func(*args, **kwargs)``.

        :returns: The :class:`~qbfutures.Future` linked to the submitted job.
        
        """
        return self.submit_ext(func, args, kwargs)
        
    def submit_ext(self, func, args=None, kwargs=None, **extra):
        """Extended submission with more control over Qube job.
        
        :param func: The function to call.
        :param list args: The positional arguments to call with.
        :param dict kwargs: The keyword arguments to call with.
        :param **extra: Values to pass through to the ``qb.Job``.
        :returns: The :class:`~qbfutures.Future` linked to the submitted job.
        
        """
        
        job = self._base_job(func, **extra)
        work = qb.Work()
        package = self._base_work_package(func, args, kwargs, extra)
        work['name'] = str(package.pop('name', '1'))
        work['package'] = utils.pack(package)
        job['agenda'] = [work]
        return self._submit(job)[0]
    
    def map(self, func, *iterables, **extra):
        """Equivalent to ``map(func, *iterables)`` except ``func`` is executed
        asynchronously on Qube.
        
        :param timeout: The number of seconds to wait for results, or ``None``.
        
        Any other keyword arguments will be passed through to the ``qb.Job``::
        
            >>> for result in Executor().map(my_function, range(10), cpus=4):
            ...     print result
        
        """
        
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
    
    def batch(self, name=None, **kwargs):
        """Start a batch process.
        
        :param str name: The name of the Qube job.
        :param \**kwargs: Other parameters for the Qube job.
        :returns: The :class:`~qbfutures.core.Batch` to use to schedule jobs in a batch.
        
        ::
            >>> with Executor().batch() as batch:
            ...     f1 = batch.submit(first_function)
            ...     f2 = batch.submit(second_function)
            ...
            >>> print f1.results()
        
        """
        if name is not None:
            kwargs['name'] = name
        job = self._base_job(None, **kwargs)
        return Batch(self, job)




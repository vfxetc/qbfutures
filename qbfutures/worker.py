import datetime
import pprint
import subprocess
import sys
import time
import traceback
import cPickle as pickle
import os
import multiprocessing

import qb

from . import utils


def execute():

    job = qb.jobobj()
    jobstate = 'complete'

    # The request work/execute/report work loop
    while True:
    
        agenda = qb.requestwork()
        # print 'AGENDA'
        # pprint.pprint(agenda)
        # print '...'

        # Non-running states.
        if agenda['status'] in ('complete', 'pending', 'blocked'):
            # complete -- no more frames
            # pending -- preempted, so bail out
            # blocked -- perhaps item is part of a dependency
            jobstate = agenda['status']
            break
        
        # Waiting; relatively rare, try again shortly.
        elif agenda['status'] == 'waiting':
            timeout = 10 # seconds
            print 'job %s will be back in %d secs' % (job['id'], timeout)
            time.sleep(timeout)
            continue

        # == Running, so execute now ==
        print '%s BEGIN %s %s' % ('='*20, agenda['name'], '='*20)

        # Assemble the command to execute
        package = utils.unpack(agenda['package'])
        func = package['func']
        args = package.get('args') or ()
        kwargs = package.get('kwargs') or {}
        
        print 'func:', repr(func)
        print 'args:', repr(args)
        print 'kwargs:', repr(kwargs)
        print '---'
        
            
        try:
            if isinstance(func, basestring):
                mod_name, func_name = func.split(':')
                mod = __import__(mod_name, fromlist=['.'])
                func = getattr(mod, func_name)
            agenda['resultpackage'] = utils.pack({'result': func(*args, **kwargs)})
            agenda['status'] = 'complete'
        except Exception as e:
            agenda['resultpackage'] = utils.pack({'exception': e})
            agenda['status'] = 'failed'
            traceback.print_exc()
                
        print agenda['resultpackage']
        print '---'
        
        # Report back the results to the Supervisor.
        qb.reportwork(agenda)

        print '%s END %s %s' % ('='*20, agenda['name'], '='*20)

    qb.reportjob(state)


debug = False


def main():
    print 'MAIN'
    
    job = {} if debug else qb.jobobj()
    jobstate = 'complete'

    # The request work/execute/report work loop
    while True:
    
        agenda = {
            'status': 'running',
            'package': {
                'func': 'qbfutures.test.work:func',
            },
        } if debug else qb.requestwork()

        # Non-running states.
        if agenda['status'] in ('complete', 'pending', 'blocked'):
            # complete -- no more frames
            # pending -- preempted, so bail out
            # blocked -- perhaps item is part of a dependency
            jobstate = agenda['status']
            break
        
        # Waiting; relatively rare, try again shortly.
        elif agenda['status'] == 'waiting':
            timeout = 10 # seconds
            print 'job %s will be back in %d secs' % (job['id'], timeout)
            time.sleep(timeout)
            continue
        
        request_pipe = os.pipe()
        response_pipe = os.pipe()
        env = dict(os.environ)
        env['QBFUTURES_PIPES'] = '%s,%s' % (request_pipe[0], response_pipe[1])
        
        interpreter = agenda['package'].get('interpreter', 'python')
        proc = subprocess.Popen(['dev', '--bootstrap', interpreter, '-m', 'qbfutures.worker'],
            close_fds=False,
            env=env
        )
        
        with os.fdopen(request_pipe[1], 'w') as request_fh:
            pickle.dump(job, request_fh, -1)
            pickle.dump(agenda, request_fh, -1)
        with os.fdopen(response_pipe[0], 'r') as response_fh:
            package = pickle.load(response_fh)
        
        agenda['resultpackage'] = package
        agenda['status'] = package.get('status', 'failed')
        
        # print 'AGENDA: %r' % agenda
        
        if not debug:
            qb.reportwork(agenda)
        
        proc.wait()
        
        if debug:
            break
    
    # print 'DONE parent'


if __name__ == '__main__':
    # print 'EXECUTE'

    request_pipe, response_pipe = [int(x) for x in os.environ['QBFUTURES_PIPES'].split(',')]
    request_fh = os.fdopen(request_pipe, 'r')
    response_fh = os.fdopen(response_pipe, 'w')
    try:
        
        job = pickle.load(request_fh)
        agenda = pickle.load(request_fh)
    
        # print 'JOB: %r' % job
        # print 'AGENDA: %r' % agenda
    
        # Assemble the command to execute
        package = utils.unpack(agenda['package'])
        func = package['func']
        args = package.get('args') or ()
        kwargs = package.get('kwargs') or {}
        
        print 'func:', repr(func)
        print 'args:', repr(args)
        print 'kwargs:', repr(kwargs)
        print '---'
    
        if isinstance(func, basestring):
            mod_name, func_name = func.split(':')
            mod = __import__(mod_name, fromlist=['.'])
            func = getattr(mod, func_name)
            
        package = {
            'result': func(*args, **kwargs),
            'status': 'complete',
        }
    
        pickle.dump(utils.pack(package), response_fh, -1)
    
    except Exception as e:
        package = {
            'exception': e,
            'status': 'failed',
        }
    
    finally:
        request_fh.close()
        pickle.dump(utils.pack(package), response_fh, -1)
        response_fh.close()
    
    # print 'DONE child'



import cPickle as pickle
import datetime
import fcntl
import multiprocessing
import os
import pprint
import subprocess
import sys
import time
import traceback

import qb

from . import utils


debug = 'DEBUG' in os.environ


def main():
    
    job = {} if debug else qb.jobobj()
    job_for_child = dict(job)
    job_for_child.pop('agenda', None)
    
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
        
        # Prepare some pipes for communicating with the subprocess.
        request_pipe = os.pipe()
        response_pipe = os.pipe()
        
        proc = subprocess.Popen(
            [
                'dev', '--bootstrap',
                agenda['package'].get('interpreter', 'python'),
                '-m', 'qbfutures.worker',
                str(request_pipe[0]), str(response_pipe[1]),
            ],
            close_fds=False
        )
        
        # Close our end of the pipes so that we will get an EOFError if the
        # pipe breaks.
        os.close(request_pipe[0])
        os.close(response_pipe[1])
        
        # Send the job and agenda to the child.
        with os.fdopen(request_pipe[1], 'w') as request_fh:
            pickle.dump(job_for_child, request_fh, -1)
            pickle.dump(agenda, request_fh, -1)
        
        # Get the response from the child.
        with os.fdopen(response_pipe[0], 'r') as response_fh:
            try:
                package = pickle.load(response_fh)
            except Exception as e:
                package = {'status': 'failed', 'exception': e}
        
        # Wait for the process to finish.
        proc.wait()
        
        agenda['resultpackage'] = package
        agenda['status'] = package.get('status', 'failed')
        
        # print 'AGENDA: %r' % agenda['resultpackage']
        
        if debug:
            break
        else:
            qb.reportwork(agenda)
    
    if debug:
        pass
        # print 'DONE parent'
    else:
        qb.reportjob(jobstate)


if __name__ == '__main__':
    # print 'EXECUTE'
    
    request_pipe, response_pipe = [int(x) for x in sys.argv[1:3]]
    request_fh = os.fdopen(request_pipe, 'r')
    response_fh = os.fdopen(response_pipe, 'w')
    
    # Just in case someone calls `exit()` which won't be caught below.
    package = {'status': 'failed'}
    
    try:
        
        # Get the job/agenda from the parent.
        job = pickle.load(request_fh)
        agenda = pickle.load(request_fh)
    
        # Assemble the command to execute
        package = utils.unpack(agenda['package'])
        func = package['func']
        args = package.get('args') or ()
        kwargs = package.get('kwargs') or {}
        
        arg_spec = ', '.join([repr(x) for x in args] + ['%s=%r' % x for x in sorted(kwargs.iteritems())])
        print '# qbfutures: calling %s(%s)' % (func, arg_spec)
        
        # If given an entrypoint-like string, parse it and import the function.
        if isinstance(func, basestring):
            mod_name, func_name = func.split(':')
            mod = __import__(mod_name, fromlist=['.'])
            func = getattr(mod, func_name)
            
        package = {
            'result': func(*args, **kwargs),
            'status': 'complete',
        }
        
    except Exception as e:
        package = {
            'exception': e,
            'status': 'failed',
        }
    
    finally:

        # Send the results to the child.
        pickle.dump(utils.pack(package), response_fh, -1)

        request_fh.close()
        response_fh.close()
    
    # print 'DONE child'



import datetime
import pprint
import subprocess
import sys
import time
import traceback
import cPickle as pickle

import qb

from . import utils


def initJob():
    job = qb.jobobj()
    # print 'JOB'
    # pprint.pprint(job)
    # print '...'
    return job


def executeJob(job):
    
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

    return jobstate

    
def cleanupJob(job, state):
    qb.reportjob(state)


def main():
    """Main entrypoint called by the "qbfutures" job type."""
    job = initJob()
    state = executeJob(job)
    cleanupJob(job, state)





import sys
import pprint
import subprocess
import datetime
import qb

def initJob():
    # Get the job object
    jobObject = qb.jobobj()

    # Gather and print the job object
    pp = pprint.PrettyPrinter(indent=4)

    print 'JOB:'
    pp.pprint(jobObject)

    return jobObject


def executeJob(jobObject):
    jobstate = 'complete'

    # The request work/execute/report work loop
    while 1:
        agendaItem = qb.requestwork()
        print agendaItem['status']

        # == Handle non-running state cases ==
        if agendaItem['status'] in ('complete', 'pending', 'blocked'):
            # complete -- no more frames
            # pending -- preempted, so bail out
            # blocked -- perhaps item is part of a dependency
            jobstate = agendaItem['status']
            print 'job %s state is now %s' % (jobObject['id'], jobstate)
            break
        # waiting -- relatively rare, try again in 30 secs.
        elif agendaItem['status'] == 'waiting':
            WAITINGTIMEOUT = 30 # seconds
            print 'job %s will be back in %d secs' % (
                jobObject['id'], WAITINGTIMEOUT)
            import time
            time.sleep(WAITINGTIMEOUT)
            continue

        # == Running, so execute now ==
        print '%s BEGIN %04d %s' % ('='*20, int(agendaItem['name']), '='*20)

        # Assemble the command to execute
        command = jobObject['package']['cmdline'].replace('QB_FRAME_NUMBER', agendaItem['name'])
        print 'COMMAND: %s' % command

        # == Execute the command ==
        if jobObject['package'].get('logdir', None) in ('', None):
            # STANDARD:
            p1 = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
            while True:
                line = p1.stdout.readline()
                if line == '': break  # EOF
                print   "[%s] %s"%(datetime.datetime.now(),line),
                sys.stdout.flush()
            p1.wait() # wait for the process to complete and set the returncode
        else:
            # ADVANCED OPTION: Additionally send the stdout to a per-frame file
            logFilename = '%s/log_%s.txt'%(jobObject['package'].get('logdir', '/tmp'), agendaItem['name'])
            f = open(logFilename, 'w', 0)   # 0 means unbuffered output
            p1 = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE)
            while True:
                line = p1.stdout.readline()
                if line == '': break  # EOF
                f.write("[%s] %s"%(datetime.datetime.now(),line))
                print   "[%s] %s"%(datetime.datetime.now(),line),
                sys.stdout.flush()
            f.close()
            p1.wait() # wait for the process to complete and set the returncode
        
        # == Set the agenda item parameters ==
        # Assess the work and update item status
        if p1.returncode == 0:
            agendaItem['status'] = 'complete'
        else:
            agendaItem['status'] = 'failed'

        # ADVANCED OPTION: Set the resultpackage to send data back
        agendaItem['resultpackage'] = { 'outputPaths': 'C:/YOUR/PATH/GOES/HERE/image'+agendaItem['name']+'.tga' }
        
        # == Report back the results to the Supervisor ==
        qb.reportwork(agendaItem)

        print '%s END %04d %s' % ('='*20, int(agendaItem['name']), '='*20)

    return jobstate

    
def cleanupJob(jobObject, state):
    qb.reportjob(state)


def main():
    jobObject = initJob()
    state    = executeJob(jobObject)
    cleanupJob(jobObject, state)
    

if __name__ == "__main__":
    main()


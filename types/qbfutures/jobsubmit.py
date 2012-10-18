#!/usr/bin/python
#
# Usage:
#    python jobsubmit.py --range 1-3 set
#    python jobsubmit.py --range 1-3 --logdir /tmp/AAA set
#    python jobsubmit.py --range 1-3 --archive "/tmp/jobArchive.xja" set
#
#

import sys, os
import optparse

import qb


def main():
    parser = optparse.OptionParser(conflict_handler="resolve", usage='usage: %prog [OPTIONS] <commandstring>')
    parser.add_option("--range"  , type="string", default='', metavar="STR" , help='framerange (i.e. "1-5")')
    parser.add_option("--subjobs", type="int"   , default=1 , metavar="INT" , help='number of subjob processes to run')
    parser.add_option("--archive", type="string", default='', metavar="FILE", help='location a job archive .xja file for use with bootstrap.py')
    parser.add_option("--logdir" , type="string", default='', metavar="DIR" , help='location of per-frame log output')
    (options, args) = parser.parse_args()

    # Check for a command
    if len(args) == 0:
        print 'ERROR: no command specified'
        sys.exit(1)

    # Build the command
    command = ' '.join(args)

    # Build the job
    job = {}
    job['name'] = 'QBFutures Test Job'
    job['cpus'] = options.subjobs
    job['prototype'] = 'qbfutures'
    #job['requirements'] = 'host.os=linux'
    job['env'] = dict(os.environ)
    
    # This job type has only one package variable
    package = {}
    job['package'] = package
    job['package']['cmdline'] = command
    job['package']['logdir'] = options.logdir

    # Build the frame list work agenda
    if options.range != '':
        agenda = qb.genframes(options.range)
        job['agenda'] = agenda

    # submit the job or create a job file archive for it
    if options.archive != '':
        # Use this option to use the bootstrap.py shortcircuit for development
        qb.archivejob(options.archive, job)
    
    else:
        # Submit the job to the Supervisor
        submittedJobs = qb.submit([job])    
        # Print out the job IDs
        print ' '.join([str(j['id']) for j in submittedJobs])


if __name__ == '__main__':
    main()


print __file__

import sys
print 'sys.path:'
for x in sys.path:
    print '\t', x
print

import os
print 'os.environ:'
for x in sorted(os.environ.iteritems()):
    print '\t', '%s: %r' % x
print

from qbfutures.worker import initJob, executeJob, cleanupJob

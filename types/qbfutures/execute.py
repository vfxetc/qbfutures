import os
import sys

if 'KS_TOOLS' not in os.environ and int(os.environ.get('SHLVL', 10)) < 3:
    print '# Resetting via bash.'
    os.execvp('bash', ['bash', '-c', '. ~/.bashrc; python %s' % __file__])
    
# Bootstrap the partial dev environment. We still can't dev on anything else,
# but atleast we have this package.
if 'QBFUTURES_DIR' in os.environ:
    sys.path.insert(0, os.environ['QBFUTURES_DIR'])

from qbfutures.worker import main
main()

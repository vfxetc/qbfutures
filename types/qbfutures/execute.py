import os
import sys

# Bootstrap the partial dev environment. We still can't dev on anything else,
# but atleast we have this package.
if 'QBFUTURES_DIR' in os.environ:
    sys.path.insert(0, os.environ['QBFUTURES_DIR'])

from qbfutures.worker import main
main()

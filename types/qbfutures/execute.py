import os
import sys

# Bootstrap the partial dev environment.
if 'QBFUTURES_PATH' in os.environ:
    sys.path.insert(0, os.environ['QBFUTURES_PATH'])

from qbfutures.worker import main
main()

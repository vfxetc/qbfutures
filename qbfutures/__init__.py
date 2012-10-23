import os

# Just to trick it into importing fir ReadTheDocs.
if os.environ.get('READTHEDOCS', None) == 'True':
    import sys.modules['qb'] = sys.modules[__name__]

from core import Executor, Future

_main = Executor()
submit = _main.submit
submit_ext = _main.submit_ext
map = _main.map

from core import Executor, Future

_main = Executor()
submit = _main.submit
submit_ext = _main.submit_ext
map = _main.map

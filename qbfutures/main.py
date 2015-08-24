import re
import sys
from optparse import OptionParser

from .core import Executor


def our_exec(src, imports):
    src = src.rstrip()
    is_exec = '\n' in src or ';' in src
    src += '\n'
    namespace = {}
    for name in imports:
        namespace[name.split('.')[0]] = __import__(name)
    if is_exec:
        eval(compile(src, '<string>', 'exec'), namespace)
    else:
        return eval(src, namespace)


def main():

    opt_parser = OptionParser()
    opt_parser.add_option('-c', '--cpus', type='int', default=1)
    opt_parser.add_option('-n', '--name')
    opt_parser.add_option('-u', '--user')
    opt_parser.add_option('-w', '--wait', action='store_true')
    opt_parser.add_option('-v', '--verbose', action='store_true')
    opt_parser.add_option('-r', '--repr', action='store_true')
    opt_parser.add_option('-i', '--imports', action='append', default=[])
    opts, args = opt_parser.parse_args()

    src = ' '.join(args)
    if src == '-':
        src = sys.stdin.read()

    executor = Executor(cpus=opts.cpus, name=opts.name or src)

    extra = {}
    if opts.user:
        extra['user'] = opts.user

    if re.match(r'^\w+(\.\w+)*:\w+', src):
        future = executor.submit_ext(src, **extra)
    else:
        future = executor.submit_ext('qbfutures.main:our_exec', [src], {'imports': opts.imports}, **extra)

    if opts.verbose:
        print 'Job ID %d' % future.job_id

    if opts.wait:
        res = future.result()
        if opts.repr:
            res = repr(res)
        print res


if __name__ == '__main__':
    main()

import os
import sys


def log(msg):
    sys.stdout.write('# qbfutures: %s\n' % msg.rstrip())
    sys.stdout.flush()
    sys.stderr.write('# qbfutures: %s\n' % msg.rstrip())
    sys.stderr.flush()


log('using type %s' % __file__)


# This is, unfortunately, tied strongly to the WesternPost environment.
if 'KS_TOOLS' not in os.environ and 'VEE_EXEC_PREFIX' not in os.environ:
    if os.environ.get('QBFUTURES_BASH_DEPTH'):
        log('Environment STILL appears bare; continuing anyways.')
    elif int(os.environ.get('SHLVL', 10)) < 3:
        log('Too much bash recursion!')
    else:
        log('Environment appears bare; sourcing ~/.bashrc')
        os.environ['QBFUTURES_BASH_DEPTH'] = 1
        os.execvpe('bash', ['bash', '-c', '. ~/.bashrc; python %s' % __file__], os.environ)


# Bootstrap the partial dev environment. We still can't dev on anything else,
# but atleast we have this package.
if 'QBFUTURES_DIR' in os.environ:
    sys.path.insert(0, os.environ['QBFUTURES_DIR'])


from qbfutures.worker import main
main()


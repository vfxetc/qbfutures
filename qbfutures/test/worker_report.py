import os
import sys
import platform
import pprint

from concurrent.futures import as_completed

import qb


def report(*args):
    return args, platform.architecture(), platform.processor(), platform.system(), platform.uname(), platform.platform()


if __name__ == '__main__':
    
    archs = set()
    for host in qb.hostinfo():

        if host['state'] != 'active':
            continue
        if 'farm' not in host['groups'].split(','):
            continue

        props = host['properties']
        props = dict(x.split('=', 1) for x in props.split(','))
        archs.add((props['host.architecture'], props['host.os'], props['host.kernel_version']))
    
    from .. import Executor
    executor = Executor()

    futures = []
    for arch, os_, kernel in sorted(archs):
        futures.append(
            executor.submit_ext(
                func='qbfutures.test.worker_report:report',
                args=(arch, os_, kernel),
                name='Python Report on %s/%s/%s' % (arch, os_, kernel),
                requirements=' && '.join((
                    'host.architecture == "%s"' % arch,
                    'host.os == "%s"' % os_,
                    'host.kernel_version == "%s"' % kernel,
                )),
            )
        )

    for future in as_completed(futures):
        try:
            res = future.result()
        except Exception as res:
            pass
        print future.job_id, future.work_id, res

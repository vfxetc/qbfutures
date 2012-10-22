from __future__ import absolute_import

from concurrent.futures import as_completed

from maya import cmds

from ..maya import Executor


def submit():
    executor = Executor(clone_environ=True, create_tempfile=True)
    with executor.batch("QBFutures Example: Get Node Types") as batch:
        for node in cmds.ls(sl=True):
            future = batch.submit_ext(get_type, [node], name='nodeType(%r)' % node)
            future.node = node
    for future in as_completed(batch.futures):
        print future.job_id, future.work_id, future.node, future.result()



def get_type(node):
    return cmds.nodeType(node)

from __future__ import absolute_import

from concurrent.futures import as_completed


from ..maya import Executor


def get_type(node):
    from maya import cmds
    return node, cmds.nodeType(node)


def submit():
    from maya import cmds
    executor = Executor(clone_environ=True, create_tempfile=True)
    with executor.batch("QBFutures Example: Get Node Types") as batch:
        for node in cmds.ls(sl=True):
            future = batch.submit_ext(get_type, [node], name='nodeType(%r)' % node)
    for future in as_completed(batch.futures):
        print future.job_id, future.work_id, future.result()


def fail():
    raise ValueError('testing failure')


if __name__ == '__main__':
    executor = Executor(name="Maya Exeception Test")
    executor.submit('qbfutures.test.maya:fail').result()
    


import sys
import os

def func(*args, **kwargs):
    return args

def maya_test():
    import maya.standalone
    maya.standalone.initialize()
    from maya import cmds
    print 'sphere:', cmds.polySphere()
    cmds.file(rename="/home/mboers/key_tools/qbfutures/test.ma")
    cmds.file(force=True, save=True, type="mayaAscii")
    return 'DONE'


def list_transforms():
    from maya import cmds
    return cmds.ls(type='transform')

def get_sys_path():
    return list(sys.path)
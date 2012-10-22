from __future__ import absolute_import

import os
import datetime

from . import core

# Test for maya.
try:
    from maya import cmds as maya_cmds, mel as maya_mel
    IN_MAYA = True
except ImportError:
    IN_MAYA = False


def preflight(package):
    
    import maya.standalone as maya_standalone
    maya_standalone.initialize()
    
    from maya import cmds
    filename = package.get('filename')
    if filename:
        print '# qbfutures.maya: opening file %r' % filename
        cmds.file(filename, open=True, force=True)
    workspace = package.get('workspace')
    if workspace:
        print '# qbfutures.maya: setting workspace %r' % workspace
        cmds.workspace(dir=workspace)


class Executor(core.Executor):
    
    def __init__(self, clone_environ=None, create_tempfile=False, **kwargs):
        super(Executor, self).__init__(**kwargs)
        
        # Pull overrides from given kwargs.
        self.filename = kwargs.get('filename')
        self.workspace = kwargs.get('workspace')
        self.version = kwargs.get('version')
        
        if create_tempfile:
            if isinstance(create_tempfile, basestring):
                self.filename = create_tempfile
            self.create_tempfile()
        if clone_environ:
            self.clone_environ()
        
        # Set a default.
        self.version = self.version or 2011
        
    def create_tempfile(self):
        if not IN_MAYA:
            raise RuntimeError('cannot create tempfile when not in Maya')

        existing = maya_cmds.file(q=True, expandName=True)
        
        # Determine what we are going to call it.
        if self.filename is None:
            base_name, base_ext = os.path.splitext(os.path.basename(existing))
            base_ext = base_ext or '.mb'
            dir_path = os.path.dirname(existing).replace('/.qbfutures', '')
            self.filename = os.path.join(dir_path, '.qbfutures', '%s.%s%s' % (
                base_name,
                datetime.datetime.utcnow().isoformat('T'),
                base_ext,
            ))
            print 'FILENAME', self.filename
        
        # Create the directory.
        dir_path = os.path.dirname(self.filename)
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)
        
        # Save the file.
        maya_cmds.file(rename=self.filename)
        _, ext = os.path.splitext(self.filename)
        maya_cmds.file(save=True, force=True, type='mayaAscii' if ext == '.ma' else 'mayaBinary')
        
        maya_cmds.file(rename=existing)
    
    def clone_environ(self):
        if not IN_MAYA:
            raise RuntimeError('cannot clone environment when not in Maya')
        if self.filename is None:
            self.filename = maya_cmds.file(q=True, expandName=True)
        if self.workspace is None:
            self.workspace = maya_cmds.workspace(q=True, rootDirectory=True)
        if self.version is None:
            self.version = int(maya_mel.eval('about -version').split()[0])
    
    def _base_package(self, kwargs):
        package = super(Executor, self)._base_package(kwargs)
        
        # Executor information.
        package['preflight'] = '%s:preflight' % __name__
        package['interpreter'] = kwargs.get('interpreter', 'maya%d_python' % self.version)
        
        # The Maya environment.
        package['filename'] = kwargs.get('filename', self.filename)
        package['workspace'] = kwargs.get('workspace', self.workspace)
        package['version'] = kwargs.get('version', self.version)
    
        return package
            
            
            
            
            
            
            
            
            
            
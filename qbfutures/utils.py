import re
import cPickle as pickle


def extend(target, *args, **kwargs):
    for arg in args:
        target.update(arg or {})
    target.update(kwargs)
    return target


def get_func(spec):
    if not isinstance(spec, basestring):
        return spec
    
    m = re.match(r'([\w\.]+):([\w]+)$', spec)
    if not m:
        raise ValueError('string funcs must be for form "package.module:function"')
    mod_name, func_name = m.groups()
    mod = __import__(mod_name, fromlist=['.'])
    return getattr(mod, func_name)


def get_func_name(spec):
    if isinstance(spec, basestring):
        return spec
    return '%s.%s' % (getattr(spec, '__module__', '__module__'), getattr(spec, '__name__', str(spec)))
    
    
def pack(package):
    res = dict(package)
    res.pop('__pickle__', None)
    res['__pickle__'] = pickle.dumps(res).encode('base64')
    return res


def unpack(package):
    if '__pickle__' in package:
        return pickle.loads(package['__pickle__'].decode('base64'))
    return dict(package)

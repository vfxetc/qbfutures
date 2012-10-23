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
    

def _clean_for_pack(x):
    if x is None:
        return x
    if isinstance(x, (int, float, str, bool)):
        return x
    if isinstance(x, unicode):
        return x.encode('utf8')
    if isinstance(x, (list, tuple, set)):
        return type(x)(_clean_for_pack(v) for v in x)
    if isinstance(x, dict):
        return type(x)((k, _clean_for_pack(v)) for k, v in x.iteritems())
    return '<<%r>>' % x


def pack(package):
    package = dict(package)
    package.pop('__pickle__', None)
    cleaned = dict((k, _clean_for_pack(v)) for k, v in package.iteritems())
    cleaned['__pickle__'] = pickle.dumps(dict(package), -1).encode('base64')
    return cleaned


def unpack(package):
    if '__pickle__' in package:
        return pickle.loads(package['__pickle__'].decode('base64'))
    return dict(package)

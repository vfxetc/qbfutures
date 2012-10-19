import cPickle as pickle


def pack(package):
    res = dict(package)
    res['__pickle__'] = pickle.dumps(res).encode('base64')
    return res


def unpack(package):
    if '__pickle__' in package:
        return pickle.loads(package['__pickle__'].decode('base64'))
    return dict(package)

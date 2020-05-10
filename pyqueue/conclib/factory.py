from pyqueue.conclib.thread_backend import ThreadBackend
from pyqueue.conclib.gevent_backend import GeventBackend
import importlib


class BackendDoesNotExistException(Exception):
    def __init__(self, backend, *args, **kwargs):
        super().__init__("Backend {} does not exist.".format(backend), *args, **kwargs)


class IncorrectBackendPathException(Exception):
    def __init__(self, path, *args, **kwargs):
        super().__init__("Path {} is not correct.".format(path), *args, **kwargs)


class BackendImportFailedException(Exception):
    def __init__(self, path, error, *args, **kwargs):
        super().__init__("Failed to import path {} due {}.".format(path, error), *args, **kwargs)


class ConcBackendFactory():
    CONC_BACKENDS = {
        "THREAD": ThreadBackend,
        "GEVENT": GeventBackend
    }

    @classmethod
    def GetBackend(cls, name="THREAD"):
        """
        :param name: for defined backend names in CONC_BACKENDS or an import path for the module and class name
                separated by comma
        :return:
        """
        backend = cls.CONC_BACKENDS.get(name)
        if not backend:
            try:
                module_path, class_name = backend.split(",")
            except:
                raise IncorrectBackendPathException(backend)

            if not module_path or not class_name:
                raise IncorrectBackendPathException(backend)

            try:
                module = importlib.import_module(module_path)
                backend = getattr(module, class_name)
            except Exception as e:
                raise BackendImportFailedException(backend, repr(e))

        return backend

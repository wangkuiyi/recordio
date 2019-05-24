import ctypes
import os
import six

path = os.path.join(os.path.dirname(__file__), "librecordio.so")
lib = ctypes.cdll.LoadLibrary(path)


def _convert_to_bytes(obj):
    if isinstance(obj, six.text_type):
        return obj.encode()
    elif isinstance(obj, six.binary_type):
        return obj
    elif obj is None:
        return obj
    else:
        return six.b(obj)


class Writer(object):
    """
    recordio writer.
    """

    def __init__(self, path):
        self._w = lib.create_recordio_writer(_convert_to_bytes(path))

    def close(self):
        lib.release_object(self._w)
        self._w = None

    def write(self, record):
        lib.recordio_write(
            self._w, ctypes.c_char_p(_convert_to_bytes(record)), len(record)
        )


class Index(object):
    """
    recordio index.
    """

    def __init__(self, path):
        self._idx = lib.create_recordio_index(_convert_to_bytes(path))

    def close(self):
        lib.release_object(self._idx)
        self._idx = None

    def num_records(self):
        return lib.recordio_index_num_records(self._idx)


class Scanner(object):
    """
    recordio reader.
    """

    def __init__(self, path, start=0, len=-1, index=None):
        if index is None:
            self._idx = Index(path)
            self._own_idx = True
        else:
            self._idx = index
            self._own_idx = False

        self._r = lib.create_recordio_reader(
            _convert_to_bytes(path), self._idx._idx, start, len
        )

    def close(self):
        if self._own_idx:
            self._idx.close()
        self._idx = None
        lib.release_object(self._r)
        self._r = None

    def record(self):
        p = ctypes.c_char_p()
        ret = ctypes.pointer(p)
        size = lib.recordio_read(self._r, ret)
        if size < 0:
            # EOF
            return None
        if size == 0:
            # empty record
            return b""

        p2 = ctypes.cast(p, ctypes.POINTER(ctypes.c_char))
        record = p2[:size]

        # memory created from C should be freed.
        lib.mem_free(ret.contents)
        return record

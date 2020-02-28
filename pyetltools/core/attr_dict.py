class AttrDict(object):
    def __init__(self, initializer=None):
        self._data=dict()
        self._initializer = initializer
        self._is_initialized = False if initializer is not None else True

    def _initialize(self):
        if self._initializer:
            self._initializer(self)

    def get(self, key):
        return self.__getattr__(key)

    def keys(self):
        return self._data.keys()

    def __getattr__(self, key):
        if not self._is_initialized:
            self._is_initialized = True
            self._initialize()
        if key in self._data:
            return self._data[key]
        else:
            # Default behaviour
            raise AttributeError(f"{key} does not exists.")

    def _add_attr(self, key, value):
        self._data[key]=value
        setattr(self, key, value)
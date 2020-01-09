from abc import abstractmethod


class DataSource:
    def __init__(self, config):
        self._config = config

    @abstractmethod
    def get_spark_dataframe(self):
        pass

    @abstractmethod
    def get_pandas_dataframe(self):
        pass

    @abstractmethod
    def get_dataframe(self):
        pass


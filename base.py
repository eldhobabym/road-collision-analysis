from sp_session import get_spark_session


class Base:
    def __init__(self):
        self._spark = get_spark_session()

    @property
    def spark(self):
        return self._spark

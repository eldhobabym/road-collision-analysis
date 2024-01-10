from pyspark.sql.functions import col

from base import Base
from utils import get_file_names

# spark = get_spark_session()


#  Reading collisiosn record
class ReadData(Base):
    def read_raw_data(self, path):
        return (
            self.spark.read.option("delimiter", ",").option("header", True).csv(path)
        ).repartitionByRange("CASE_ID")


class ReadDelata(Base):
    def read_delta_data(self, table_name, path):
        self.spark.sql(
            f"CREATE TABLE IF NOT EXISTS {table_name} USING delta LOCATION '{path}'"
        )

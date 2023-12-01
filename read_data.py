from sp_session import get_spark_session


from pyspark.sql.functions import col

from utils import get_file_names

# spark = get_spark_session()


#  Reading collisiosn record
class ReadData:
    def __init__(self):
        self.spark = get_spark_session()

    def read_raw_data(self, path):
        return (
            self.spark.read.option("delimiter", ",").option("header", True).csv(path)
        ).repartitionByRange("CASE_ID")

   
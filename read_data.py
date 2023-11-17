from sp_session import get_spark_session


from pyspark.sql.functions import col

# spark = get_spark_session()


#  Reading collisiosn record
class ReadData:
    def __init__(self):
        self.spark = get_spark_session()

    def read_collision_record_data(self):
        return (
            self.spark.read.option("delimiter", ",")
            .option("header", True)
            .csv(
                "/home/ubuntu/project/DE/road-collision-analysis/data/20160924/20160924_CollisionRecords.txt"
            )
        ).repartitionByRange("CASE_ID")

    def read_party_record_data(self):
        return (
            self.spark.read.option("delimiter", ",")
            .option("header", True)
            .csv(
                "/home/ubuntu/project/DE/road-collision-analysis/data/20160924/20160924_PartyRecords.txt"
            ).repartitionByRange("CASE_ID")
        )

    def read_victim_record_data(self):
        return (
            self.spark.read.option("delimiter", ",")
            .option("header", True)
            .csv(
                "/home/ubuntu/project/DE/road-collision-analysis/data/20160924/20160924_VictimRecords.txt"
            ).repartitionByRange("CASE_ID")
        )

from read_data import ReadData
from pyspark.sql.functions import col

from pyspark.sql.utils import AnalysisException


class Profiling(ReadData):
    def coll_profiling(self, df, temp_table):
        temp_table_name = temp_table
        profiling_arry = []
        collition_record_df = df

        # Null count
        collition_record_df.createOrReplaceTempView(temp_table_name)
        self.spark.sql(f"CACHE TABLE {temp_table_name}")

        # collition_record_df.columns
        min_val = None
        max_val = None
        for col_name in collition_record_df.columns:
            null_count_df = self.spark.sql(
                f"select count(*) from {temp_table_name} where {col_name} is NULL"
            )
            unique_values = self.spark.sql(
                f"SELECT COUNT(DISTINCT {col_name}) from {temp_table_name} where {col_name} is NULL"
            )

            try:
                #               Checking for date format
                self.spark.sql(
                    f"SELECT {col_name} from {temp_table_name} WHERE TRY_CAST({col_name} as DATE)"
                )
                min_val = self.spark.sql(
                    f"SELECT MIN({col_name}) from {temp_table_name}"
                ).first()[0]
                max_val = self.spark.sql(
                    f"SELECT MAX({col_name}) from {temp_table_name}"
                ).first()[0]
            except AnalysisException as e:
                try:
                    # Checking for numeric format
                    self.spark.sql(
                        f"SELECT CAST({col_name} AS DOUBLE) FROM {temp_table_name}"
                    )
                    min_val = self.spark.sql(
                        f"SELECT MIN({col_name}) from {temp_table_name}"
                    ).first()[0]
                    max_val = self.spark.sql(
                        f"SELECT MAX({col_name}) from {temp_table_name}"
                    ).first()[0]
                except AnalysisException as e:
                    pass

            # max_val = ""

            prof_dict = {
                "column_name": col_name,
                "null_count": null_count_df.count(),
                "unique_values": unique_values.first()[0],
                "min_val": min_val,
                "max_val": max_val,
            }

            profiling_arry.append(prof_dict)

        final_df = self.spark.createDataFrame(profiling_arry)
        output_path = f"/home/ubuntu/project/DE/road-collision-analysis/output/{temp_table}"

# Write the DataFrame to a CSV file
        final_df.write.csv(output_path, header=True, mode="overwrite")
        self.spark.sql(f"UNCACHE TABLE {temp_table_name}")


profile = Profiling()

collision_df = profile.read_collision_record_data()
victim_df = profile.read_victim_record_data()
party_df = profile.read_party_record_data()


profile.coll_profiling(collision_df, "collision")
profile.coll_profiling(victim_df, "victim")
profile.coll_profiling(party_df, "party")

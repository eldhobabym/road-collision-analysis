from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


def get_spark_session():
    builder = (
        SparkSession.builder.appName("MyApp")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
    )

    return configure_spark_with_delta_pip(builder).getOrCreate()

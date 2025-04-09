# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import current_timestamp


# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------


pit_stops_df = spark.read \
.schema(pit_stops_schema) \
.option("multiLine", True) \
.json("s3://nimisha2-test-bucket/F1Analysis/pit_stops.json")

# COMMAND ----------

pitstops_with_columns_df = pit_stops_df.withColumnRenamed("raceId", "race_id") \
                                    .withColumnRenamed("driverId","driver_id")\
                                    .withColumn("ingestion_time", current_timestamp()) 

# COMMAND ----------

pitstops_with_columns_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pitstops")

display(spark.read.parquet("dbfs:/formula1DB/pitstops"))
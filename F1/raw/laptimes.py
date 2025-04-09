# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------


lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])



# COMMAND ----------

lap_times_df = spark.read \
.schema(lap_times_schema) \
.csv("s3://nimisha2-test-bucket/F1Analysis/lap_times.csv")

# COMMAND ----------


from pyspark.sql.functions import current_timestamp


# COMMAND ----------


final_df = lap_times_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp())


# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.laptimes")
display(spark.read.parquet("dbfs:/formula1DB/laptimes"))
# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType


# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("positionText", StringType(), True),
                                    StructField("positionOrder", IntegerType(), True),
                                    StructField("points", FloatType(), True),
                                    StructField("laps", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestLapSpeed", FloatType(), True),
                                    StructField("statusId", StringType(), True)])



# COMMAND ----------


results_df = spark.read \
.schema(results_schema) \
.json("s3://nimisha2-test-bucket/F1Analysis/results.json")


# COMMAND ----------

from pyspark.sql.functions import col, concat, current_timestamp, lit


# COMMAND ----------

results_with_columns_df = results_df.withColumnRenamed("resultId", "result_id") \
                                    .withColumnRenamed("raceId", "race_id") \
                                    .withColumnRenamed("driverId","driver_id")\
                                    .withColumnRenamed("constructorId","constructor_id")\
                                    .withColumnRenamed("positionText","position_text")\
                                    .withColumnRenamed("positionOrder","position_order")\
                                    .withColumnRenamed("fastestLap","fastest_lap")\
                                    .withColumnRenamed("fastestLapTime","fastest_lap_time")\
                                    .withColumnRenamed("fastestLapSpeed","fastest_lap_speed")\
                                    .withColumn("ingestion_time", current_timestamp()) 


# COMMAND ----------

results_final_df = results_with_columns_df.drop(col("statusId"))


# COMMAND ----------

results_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.results")

display(spark.read.parquet("dbfs:/formula1DB/results"))

# COMMAND ----------

dbutils.fs.rm("dbfs:/mnt/formula1dl/processed/", recurse=True)


# COMMAND ----------


# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, TimestampType
from pyspark.sql.functions import col, current_timestamp, to_timestamp, concat, lit

# COMMAND ----------


race_schema = StructType([
    StructField("raceId", IntegerType(), True),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("date", DateType(), True),
    StructField("time", StringType(), True),  # If 'time' is in HH:MM:SS format, consider changing it to TimestampType()
    StructField("url", StringType(), True)
])


# COMMAND ----------


race_df = spark.read.option("header", "true").schema(race_schema).csv("s3://nimisha2-test-bucket/F1Analysis/races.csv")


# COMMAND ----------

race_renamed_df = race_df.withColumn("ingestion_date", current_timestamp()) \
                                  .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))


# COMMAND ----------

races_selected_df = race_renamed_df.select(col('raceId').alias('race_id'), 
                                           col('year').alias('race_year'), 
                                            col('round'), 
                                            col('date'), 
                                            col('circuitId').alias('circuit_id'),
                                            col('name'), col('ingestion_date'), col('race_timestamp'))


# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

races_selected_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.race")

display(spark.read.parquet("dbfs:/formula1DB/race"))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.
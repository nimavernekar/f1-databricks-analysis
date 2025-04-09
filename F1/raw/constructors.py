# Databricks notebook source
constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"


# COMMAND ----------

constructor_df = spark.read \
.schema(constructors_schema) \
.json("s3://nimisha2-test-bucket/F1Analysis/constructors.json")

# COMMAND ----------

from pyspark.sql.functions import col,current_timestamp

# COMMAND ----------

constructor_dropped_df = constructor_df.drop(col('url'))

# COMMAND ----------

constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                             .withColumnRenamed("constructorRef", "constructor_ref") \
                                             .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

constructor_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructor")

# COMMAND ----------

display(spark.read.parquet("dbfs:/formula1DB/constructor"))

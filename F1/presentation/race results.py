# Databricks notebook source
circuits_df = spark.read.parquet("dbfs:/formula1DB/circuits") \
    .withColumnRenamed("location", "circuit_location")



# COMMAND ----------


races_df = spark.read.parquet("dbfs:/formula1DB/race") \
    .withColumnRenamed("year", "race_year")\
    .withColumnRenamed("name", "race_name")\
    .withColumnRenamed("date", "race_date")

# COMMAND ----------


drivers_df = spark.read.parquet("dbfs:/formula1DB/drivers") \
    .withColumnRenamed("name", "driver_name")\
    .withColumnRenamed("number", "driver_number")\
    .withColumnRenamed("nationality", "driver_nationality")

# COMMAND ----------

constructors_df = spark.read.parquet("dbfs:/formula1DB/constructor") \
    .withColumnRenamed("name", "team")

# COMMAND ----------

results_df = spark.read.parquet("dbfs:/formula1DB/results") \
    .withColumnRenamed("time", "race_time")

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner") \
.select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)


# COMMAND ----------

race_results_df = results_df.join(race_circuits_df, results_df.race_id == race_circuits_df.race_id) \
                            .join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
                            .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id)


# COMMAND ----------

from pyspark.sql.functions import current_timestamp


final_df = race_results_df.select("race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality",
                                 "team", "grid", "fastest_lap", "race_time", "points", "position") \
                          .withColumn("created_date", current_timestamp())

# COMMAND ----------

display(final_df.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))


# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_results")


# COMMAND ----------


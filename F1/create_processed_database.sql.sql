-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/formula1dl/processed"




-- COMMAND ----------

DESC DATABASE f1_processed;


-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "/mnt/formula1dl/presentation"


-- COMMAND ----------

show databases

-- COMMAND ----------

select current_database()

-- COMMAND ----------

use f1_processed

-- COMMAND ----------

show tables

-- COMMAND ----------

select *,concat(driver_ref,'-',code ) from drivers
where (nationality = "British"
and dob >="1990-01-01") OR  nationality="Indian"
order by dob desc

-- COMMAND ----------

desc drivers;


-- COMMAND ----------


# Databricks notebook source
from pyspark.sql.functions import datediff,avg
from pyspark.sql.functions import *
from pyspark.sql.types import ArrayType,IntegerType

# COMMAND ----------

# MAGIC %md ####Read in Dataset####

# COMMAND ----------

df_lagtimes = spark.read.csv('s3://columbia-gr5069-main/raw/lap_times.csv', header=True)

# COMMAND ----------

display(df_lagtimes)

# COMMAND ----------

df_drivers = spark.read.csv('s3://columbia-gr5069-main/raw/drivers.csv',header=True)

# COMMAND ----------

display(df_drivers)

# COMMAND ----------

# MAGIC %md ####Transform Data ####

# COMMAND ----------

df_drivers = df_drivers.withColumn("age", datediff(current_date(),col("dob"))/365)

# COMMAND ----------

df_drivers = df_drivers.withColumn("age", df_drivers['age'].cast(IntegerType()))

# COMMAND ----------

display(df_drivers)

# COMMAND ----------

df_lap_drivers = df_drivers.select('driverid','driverRef','code','forename','surname','nationality','age').join(df_lagtimes, on=['driverid'])
display(df_lap_drivers)

# COMMAND ----------

df_races = spark.read.csv('s3://columbia-gr5069-main/raw/races.csv',header=True)

# COMMAND ----------

display(df_races)

# COMMAND ----------

df_lap_drivers = df_lap_drivers.join(df_races, on='raceid')

# COMMAND ----------

display(df_lap_drivers)

# COMMAND ----------

df_lap_drivers = df_races.select('raceId', 'year', 'name').join(df_lap_drivers, on = ['raceId']).drop('raceId', 'driverId')

# COMMAND ----------

display(df_lap_drivers)

# COMMAND ----------

# MAGIC %md ####Aggregate by Age####

# COMMAND ----------

df_agg_age = df_lap_drivers.groupby('age').agg(avg("milliseconds"))

# COMMAND ----------

display(df_agg_age)

# COMMAND ----------

# MAGIC %md ####Storing Data in S3

# COMMAND ----------

df_agg_age.write.csv('s3://sc5088-gr5069/processed/inClass/laptimes_by_age.csv')

# COMMAND ----------



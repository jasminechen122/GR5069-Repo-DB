# Databricks notebook source
#import packages and functions
from pyspark.sql.functions import datediff,avg
from pyspark.sql.functions import *
from pyspark.sql.types import ArrayType,IntegerType
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md ####1. What was the average time each driver spent at the pit stop for each race?####

# COMMAND ----------

#load pit dataset in
df_pit = spark.read.csv('s3://columbia-gr5069-main/raw/pit_stops.csv',header=True)
display(df_pit)

# COMMAND ----------

#we want to know average time for each driver in each race
#Therefore, we groupby unique driverid and race id, then we calculate the average using the milliseconds column
df_Q1 = df_pit.groupby('driverId','raceId').agg(avg('milliseconds').alias('avg_pit_time(ms)'))#rename column to a more understandable way
display(df_Q1)

# COMMAND ----------

# MAGIC %md ####2. Rank the average time spent at the pit stop in order of who won each race?####

# COMMAND ----------

#Load the result dataset in
df_results =  spark.read.csv('s3://columbia-gr5069-main/raw/results.csv',header=True)
display(df_results)

# COMMAND ----------

#select the important info we needed and then join the result data with the average pit time data
#sort the data using  driver's rank 
df_Q2 = df_results.select('positionOrder','driverId').join(df_Q1, on=['driverId']).sort(df_results.positionOrder.asc())
display(df_Q2)

# COMMAND ----------

# MAGIC %md ####3. Insert the missing code (e.g: ALO for Alonso) for drivers based on the 'drivers' dataset####

# COMMAND ----------

#load drivers dataset 
df_drivers =  spark.read.csv('s3://columbia-gr5069-main/raw/drivers.csv',header=True)
display(df_drivers)

# COMMAND ----------

#extract the first three letter of the surname into code and turned them into upper case
df_Q3 = df_drivers.withColumn('code',upper(substring(df_drivers.surname,1,3)))
display(df_Q3)

# COMMAND ----------

# MAGIC %md ####4. Who is the youngest and oldest driver for each race? Create a new column called “Age”####

# COMMAND ----------

#Create a new column in driver named age
df_drivers = df_drivers.withColumn("Age", datediff(current_date(),col("dob"))/365)
#select only necessary info we needed
df_drivers_name_age = df_drivers.select('driverId','forename','surname', 'Age')
#join results data with drivers data
df_name_age = df_results.select('driverId','raceId').join(df_drivers_name_age, on=['driverId'])

# COMMAND ----------

#calculate youngest and oldest age for each race
df_max_age = df_name_age.groupby('raceId').agg(max('Age'))
df_min_age = df_name_age.groupby('raceId').agg(min('Age'))
display(df_max_age)

# COMMAND ----------

#add max and min age to the data
df_driver_max_age = df_name_age.join(df_max_age,on=['raceId'])
df_driver_min_max_age = df_driver_max_age.join(df_min_age,on=['raceId'])
display(df_driver_min_max_age)                                           

# COMMAND ----------

#filter data based on if the rows meet the youngest or the oldest age for each race
df_driver_max_age = df_name_age.join(df_max_age,on=['raceId'])
df_driver_min_max_age = df_driver_max_age.join(df_min_age,on=['raceId'])
df_driver_min_max_age = df_driver_min_max_age.filter((df_driver_min_max_age['Age']==df_driver_min_max_age['max(Age)'])|(df_driver_min_max_age['Age']==df_driver_min_max_age['min(Age)']))
#cast age into integer
df_driver_min_max_age = df_driver_min_max_age.withColumn("Age", df_driver_min_max_age['Age'].cast(IntegerType()))
df_driver_min_max_age = df_driver_min_max_age.withColumn("max(Age)", df_driver_min_max_age['max(Age)'].cast(IntegerType()))
df_driver_min_max_age = df_driver_min_max_age.withColumn("min(Age)", df_driver_min_max_age['min(Age)'].cast(IntegerType())) 
df_Q4 = df_driver_min_max_age.select('raceId','driverId','forename','surname','Age').sort('raceId')
display(df_Q4)

# COMMAND ----------

# MAGIC %md ####5.  For a given race, which driver has the most wins and losses?####

# COMMAND ----------

#load standing data in
df_standings =  spark.read.csv('s3://columbia-gr5069-main/raw/driver_standings.csv',header=True)
display(df_standings)

# COMMAND ----------

#select only the necessary columns we needed
df_win = df_standings.select('driverId','raceId','wins')

# COMMAND ----------

#join standing data with drivers data so we can know their names
df_wins_names = df_drivers.select('driverId','forename','surname').join(df_win, on=['driverId'])
display(df_wins_names)

# COMMAND ----------

#I used https://www.geeksforgeeks.org/pyspark-window-functions/ as a reference to understand window function
#using window function to calculate previous wins records for each race and races before that
prev_wins = Window.partitionBy('raceId').orderBy(col('wins').desc())
#created a new column to record previous wins counts
df_prev_wins = df_wins_names.withColumn('wins_count', row_number().over(prev_wins))
#We want to leave rows where wins_counts is equal to one in the dataframe so we have driver with the most win now
df_prev_wins = df_prev_wins.filter(col('wins_count')==1).drop(col('wins_count'))
#rename columns so they wouln't be messed up when we do our join later
df_prev_wins = df_prev_wins.withColumnRenamed('driverId','Most_win_driverId').withColumnRenamed('forename','Most_win_forename').withColumnRenamed('surname','Most_win_surname')
display(df_prev_wins)

# COMMAND ----------

#using window function to identify the person with the least win
prev_losses = Window.partitionBy('raceId').orderBy(col('wins').asc())
df_prev_losses = df_wins_names.withColumn('losses_count', row_number().over(prev_losses)).filter(col('losses_count')==1).drop(col('losses_count'))
df_prev_losses = df_prev_losses.withColumnRenamed('wins','least_win').withColumnRenamed('driverId','Least_win_driverId').withColumnRenamed('forename','Least_win_forename').withColumnRenamed('surname','Least_win_surname')
display(df_prev_losses)

# COMMAND ----------

#join previous two data to a single one
df_Q5 = df_prev_wins.join(df_prev_losses,on='raceId')
df_Q5 = df_Q5.select('raceId','Most_win_forename','Most_win_surname','wins','Least_win_forename','Least_win_surname','least_win')
display(df_Q5)

# COMMAND ----------

# MAGIC %md ####6. How many distinct nationalities were invovled in each race?####

# COMMAND ----------

#select necessary columns we needed
df_nationality = df_drivers.select('driverId','nationality')
display(df_nationality)

# COMMAND ----------

#join nationality data with race data and select the columns we are interested in
df_race_nation = df_results.join(df_nationality,on='driverId')
df_race_nation = df_race_nation.select('raceId','nationality')
display(df_race_nation)

# COMMAND ----------

#count the number of each nationality in each race
df_Q6 = df_race_nation.groupby('raceId','nationality').agg(count('nationality'))
df_Q6 = df_Q6.withColumnRenamed('count(nationality)','Participant_count').sort('raceId',ascending=True)
display(df_Q6)

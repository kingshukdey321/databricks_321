# Databricks notebook source
# MAGIC %sql
# MAGIC select * from workspace.silver.silver_airports
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

df = spark.read.format("delta").load("/Volumes/workspace/bronze/bronzevolume/airports/data/")

df = df.drop("_rescued_data")\
    .withColumn("modifiedDate", current_timestamp())

display(df)


# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

df = spark.read.format("delta").load("/Volumes/workspace/bronze/bronzevolume/customers/data/")

df = df.drop("_rescued_data")\
    .withColumn("modifiedDate", current_timestamp())

display(df)


# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
df = spark.read.format("delta").load("/Volumes/workspace/bronze/bronzevolume/flights/data")
#display(df)

df1 =df.withColumn("flight_date", to_date(col("flight_date"),"yyyy-M-d"))\
    .withColumn("modifiedDate", current_timestamp())\
    .drop("_rescued_data")
display(df1)

# COMMAND ----------

import dlt 
from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

@dlt.table(
    name = "stage_bookings"
)

def stage_bookings():
    df = spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronzevolume/bookings/data")
    return df
    



# COMMAND ----------

@dlt.view(
    name = "trans_bookings"
)
def trans_bookings():
    df = spark.readStream.table("stage_bookings")
    
    df = df.withColumn("amount", col("amount").cast("double"))\
           .withColumn("modifiedDate", current_timestamp())\
           .withColumn("booking_date", to_date(col("booking_date"),"yyyy-M-d"))\
           .drop("_rescued_data")
    return df
    



# COMMAND ----------

rules = {
    "rule1" : "booking_id IS NOT NULL",
    "rele2" : "passenger_id IS NOT NULL"
}

# COMMAND ----------

@dlt.table(
    name = "silver_bookings"
)

@dlt.expect_all_or_drop(rules)
def silver_bookings():
    df = spark.readStream.table("trans_bookings")
    return df


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_user();

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT USE CATALOG ON CATALOG workspace TO `kingshukdey321@gmail.com`;
# MAGIC GRANT USE SCHEMA ON SCHEMA workspace.default TO `kingshukdey321@gmail.com`;
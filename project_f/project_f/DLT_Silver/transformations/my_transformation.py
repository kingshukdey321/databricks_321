#Booking data for silver

import dlt 
from pyspark.sql.functions import *
from pyspark.sql.types import *


@dlt.table(
    name = "stage_bookings"
)

def stage_bookings():
    df = spark.readStream.format("delta")\
        .load("/Volumes/workspace/bronze/bronzevolume/bookings/data")
    return df
    

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
    

rules = {
    "rule1" : "booking_id IS NOT NULL",
    "rele2" : "passenger_id IS NOT NULL"
}

@dlt.table(
    name = "silver_bookings"
)

@dlt.expect_all_or_drop(rules)
def silver_bookings():
    df = spark.readStream.table("trans_bookings")
    return df
  
#############################################################################
#Flights data for silver
@dlt.view(
    name ="trans_flights"
)

def trans_flights():
    df = spark.readStream.format("delta").load("/Volumes/workspace/bronze/bronzevolume/flights/data")
    df = df.withColumn("modifiedDate", current_timestamp())\
           .drop("_rescued_data")
    return df

dlt.create_streaming_table("silver_flights")

dlt.create_auto_cdc_flow(
  target = "silver_flights",
  source = "trans_flights",
  keys = ["flight_id"],
  sequence_by = "modifiedDate",
  stored_as_scd_type = 1
)

#############################################################################
#Customers data for silver

@dlt.view(
    name="trans_customers"
)
def trans_customers():
    df = spark.readStream.format("delta").load("/Volumes/workspace/bronze/bronzevolume/customers/data")
    df = df.withColumn("modifiedDate", current_timestamp())\
           .drop("_rescued_data")
    return df

dlt.create_streaming_table("silver_customers")

dlt.create_auto_cdc_flow(
  target = "silver_customers",
  source = "trans_customers",
  keys = ["passenger_id"],
  sequence_by = "modifiedDate",
  stored_as_scd_type = 1
)

#############################################################################
#Airports data for silver

@dlt.view(
    name="trans_airports"
)
def trans_airports():
    df = spark.readStream.format("delta").load("/Volumes/workspace/bronze/bronzevolume/airports/data")
    df = df.withColumn("modifiedDate", current_timestamp())\
           .drop("_rescued_data")
    return df

dlt.create_streaming_table("silver_airports")

dlt.create_auto_cdc_flow(
  target = "silver_airports",
  source = "trans_airports",
  keys = ["airport_id"],
  sequence_by = "modifiedDate",
  stored_as_scd_type = 1
)
#############################################################################
#silver business

@dlt.table(
    name = "silver_business"
)
def silver_business():
    df = dlt.readStream("silver_bookings")\
            .join(dlt.readStream("silver_flights"),["flight_id"])\
            .join(dlt.readStream("silver_customers"),["passenger_id"])\
            .join(dlt.readStream("silver_airports"),["airport_id"])\
            .drop("modifiedDate")
    return df

#############################################################################
#mat view

@dlt.table(
    name="silver_business_mat"
)
def silver_business_mat():
    df = dlt.read("silver_business")
    return df
            
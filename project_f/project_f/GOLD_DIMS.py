# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from workspace.silver.silver_flights;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Parameters**

# COMMAND ----------

# Key column list
#key_col_list = eval(dbutils.widgets.get("keycols"))
#key_col_list

# CDC Column
#cdc_col = eval(dbutils.widgets.get("cdccol"))

#Back_Date_Refresh
#back_date_refresh = eval(dbutils.widgets.get("backdate_refresh"))

# Source object
#source_object = eval(dbutils.widgets.get("source_object"))

# Source object
#source_schema = eval(dbutils.widgets.get("source_schema"))

#target_schema="gold"

#terget_table = "dimFlights"

#surrogate_key = "DimFlightKey"


# COMMAND ----------

# Key column list
#key_col_list = eval(dbutils.widgets.get("keycols"))
#key_col_list

# CDC Column
#cdc_col = eval(dbutils.widgets.get("cdccol"))

#Back_Date_Refresh
#back_date_refresh = eval(dbutils.widgets.get("backdate_refresh"))

# Source object
#source_object = eval(dbutils.widgets.get("source_object"))

# Source object
#source_schema = eval(dbutils.widgets.get("source_schema"))

#target_schema="gold"

#terget_table = "dimAirports"

#surrogate_key = "DimAirportsKey"


# COMMAND ----------

# Key column list
key_col_list = eval(dbutils.widgets.get("keycols"))
key_col_list

# CDC Column
cdc_col = eval(dbutils.widgets.get("cdccol"))

#Back_Date_Refresh
back_date_refresh = eval(dbutils.widgets.get("backdate_refresh"))

# Source object
source_object = eval(dbutils.widgets.get("source_object"))

# Source object
source_schema = eval(dbutils.widgets.get("source_schema"))

target_schema="gold"

terget_table = "dimPassengers"

surrogate_key = "DimPassengersKey"


# COMMAND ----------

#Key column
dbutils.widgets.text("keycols","")

#CDC Column
dbutils.widgets.text("cdccol","")

#Back_Date_Refresh
dbutils.widgets.text("backdate_refresh","")

# Source object
dbutils.widgets.text("source_object","")

# Source schema
dbutils.widgets.text("source_schema","")

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Fetching Parameters**

# COMMAND ----------

key_col_list

# COMMAND ----------

# MAGIC %md
# MAGIC ## INCREMENTAL DATA INGESTION

# COMMAND ----------

if len(back_date_refresh) == 0:
  
    if spark.catalog.tableExists(f"workspace.{target_schema}.{terget_table}"):
        
      last_load = spark.sql(f"select max({cdc_col}) from workspace.{target_schema}.{terget_table}").collect()[0][0]
    else:
      last_load = "1900-01-01 00:00:00"
else:
    last_load = back_date_refresh
  
      
last_load

# COMMAND ----------

last_load

# COMMAND ----------

# DBTITLE 1,Cell 10
df_src = spark.sql(f"select * from {source_schema}.{source_object} where {cdc_col} >= '{str(last_load)}'") # Don't chain .display() to assignment.display()


# COMMAND ----------

# MAGIC %md
# MAGIC #### OLD vs NEW records

# COMMAND ----------

if spark.catalog.tableExists(f"workspace.{target_schema}.{terget_table}"):
  
  # key column string for Incrimental
  key_col_str_incrimental = ', '.join(key_col_list)

  df_tgt = spark.sql(f"select {key_col_str_incrimental},{surrogate_key}, create_date, update_date from workspace.{target_schema}.{terget_table}")
else:
    # key column string for Initial
    key_column_list_init = [f"'' as {i}" for i in key_col_list]
    key_column_list_init = ', '.join(key_column_list_init)

    df_tgt = spark.sql(f"""select {key_column_list_init}, CAST('0' as int) as {surrogate_key}, CAST('1900-01-01 00:00:00' as timestamp) as update_date, CAST('1900-01-01 00:00:00' as timestamp) as create_date where 1=0""")



# COMMAND ----------

df_tgt.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join Condition

# COMMAND ----------

join_condition = ' AND '.join([f"src.{i} = trg.{i}" for i in key_col_list])
join_condition

# COMMAND ----------


df_src.createOrReplaceTempView("src") 
df_tgt.createOrReplaceTempView("trg")

df_join = spark.sql(f"""
                    select
                    src.*,
                    trg.{surrogate_key},
                    trg.create_date,
                    trg.update_date
                    from src
                    left join trg
                    on {join_condition}""")




# COMMAND ----------

df_join.display()

# COMMAND ----------

df_old = df_join.filter(col(f'{surrogate_key}').isNotNull())
df_new = df_join.filter(col(f'{surrogate_key}').isNull())

# COMMAND ----------

df_old.display()
df_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### OLD DF (only update the update_date with current timestamp)

# COMMAND ----------

df_old_enr = df_old.withColumn("update_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #### New DF 

# COMMAND ----------

if spark.catalog.tableExists(f"workspace.{target_schema}.{terget_table}"):
    max_surrogate_key = spark.sql(f"select max({surrogate_key}) from workspace.{target_schema}.{terget_table}").collect()[0][0]
    df_new_enr = df_new.withColumn(f'{surrogate_key}', lit(max_surrogate_key)+lit(1)+monotonically_increasing_id())\
                   .withColumn("create_date", current_timestamp())\
                   .withColumn("update_date", current_timestamp())
    
else:
    max_surrogate_key = 0
    df_new_enr = df_new.withColumn(f'{surrogate_key}', lit(max_surrogate_key)+lit(1)+monotonically_increasing_id())\
                   .withColumn("create_date", current_timestamp())\
                   .withColumn("update_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ## union old and new record

# COMMAND ----------

df_union = df_old_enr.unionByName(df_new_enr)

# COMMAND ----------

df_union.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### UPSERT

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists(f"workspace.{target_schema}.{terget_table}"):
    dlt_obj = DeltaTable.forName(spark, f"workspace.{target_schema}.{terget_table}")
    dlt_obj.alias("trg").merge(df_union.alias("src"), f"trg.{surrogate_key} = src.{surrogate_key}")\
        .whenMatchedUpdateAll(condition= f"src.{cdc_col} >= trg.{cdc_col}")\
        .whenNotMatchedInsertAll()\
        .execute()



else:
    df_union.write.format("delta").mode("overwrite")\
        .saveAsTable(f"workspace.{target_schema}.{terget_table}")


# COMMAND ----------

key_col_string = ', '.join(key_col_list)


# COMMAND ----------

key_column_list_init = [f"'' as {i}" for i in key_col_list]
key_column_list_init = ', '.join(key_column_list_init)

# COMMAND ----------


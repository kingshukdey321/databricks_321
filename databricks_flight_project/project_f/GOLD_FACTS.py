# Databricks notebook source
catalog = "workspace"

# CDC Column
cdc_col = "modifiedDate"

#Back_Date_Refresh
back_date_refresh = ""

# Source object
source_object = "silver_bookings"

# Source object
source_schema = "silver"

fact_table =f"{catalog}.{source_schema}.{source_object}"

target_schema="gold"

terget_table = "FactBooking"

fact_key_cols = ["DimPassengersKey", "DimFlightKey", "DimAirportsKey", "booking_date"]

# COMMAND ----------

dimensions = [
    {
        "table": f"{catalog}.{target_schema}.dimpassengers",
        "alias": "dimpassengers",
        "join_keys": [("passenger_id", "passenger_id")], #fact_col and Dim col
        "surrogate_key": "DimPassengersKey"
    },
    {
        "table": f"{catalog}.{target_schema}.dimflights",
        "alias": "dimflights",
        "join_keys": [("flight_id", "flight_id")], #fact_col and Dim col
        "surrogate_key": "DimFlightKey"
    },
    {
        "table": f"{catalog}.{target_schema}.dimairports",
        "alias": "dimairports",
        "join_keys": [("airport_id", "airport_id")], #fact_col and Dim col
        "surrogate_key": "DimAirportsKey"
    }
]

fact_columns = ["amount", "booking_date", "modifiedDate", "booking_date"]

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

def generate_fact_query_incremantal(fact_table, dimensions, fact_columns, cdc_col, processing_date):
  fact_alias ="f"

  select_cols = [f"{fact_alias}.{col}" for col in fact_columns]

  join_clauses = []
  for dim in dimensions:
    table_full = dim["table"]
    alias = dim["alias"]
    table_name = table_full.split(".")[-1]
    surrogate_key = f"{alias}.{dim['surrogate_key']}"
    select_cols.append(surrogate_key)
    

    on_conditions = [f"{fact_alias}.{fk} = {alias}.{dk}" for fk, dk in dim["join_keys"]]
    
    join_clouse = f"LEFT JOIN {table_full} {alias} ON " + " AND ".join(on_conditions)
    join_clauses.append(join_clouse)
    

  select_clause = ",\n ".join(select_cols)
  joins = "\n".join(join_clauses)
  where_clause = f"{fact_alias}.{cdc_col} >= '{processing_date}'"
  query = f"""
  SELECT
    {select_clause}
  FROM
    {fact_table} {fact_alias}
  {joins}
  WHERE {where_clause}
  """.strip()

  return query

  
    
  
  

    
    

# COMMAND ----------

query = generate_fact_query_incremantal(fact_table, dimensions, fact_columns, cdc_col, last_load)


# COMMAND ----------

df_fact = spark.sql(query)

# COMMAND ----------

df_fact.display()

# COMMAND ----------

fact_key_cols

# COMMAND ----------

fact_key_cols_str = " AND ".join([f"src.{col} = trg.{col}" for col in fact_key_cols])
fact_key_cols_str

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists(f"workspace.{target_schema}.{terget_table}"):
    dlt_obj = DeltaTable.forName(spark, f"workspace.{target_schema}.{terget_table}")
    dlt_obj.alias("trg").merge(df_fact.alias("src"), fact_key_cols_str)\
        .whenMatchedUpdateAll(condition= f"src.{cdc_col} >= trg.{cdc_col}")\
        .whenNotMatchedInsertAll()\
        .execute()



else:
    df_fact.write.format("delta").mode("overwrite")\
        .saveAsTable(f"workspace.{target_schema}.{terget_table}")

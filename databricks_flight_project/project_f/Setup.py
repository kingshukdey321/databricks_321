# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE volume workspace.raw.rowvolume

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/workspace/raw/rowvolume/rowdata")

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/workspace/raw/rowvolume/rowdata/airports")
# Databricks notebook source
df = spark.range(0, 100)

# COMMAND ----------

df.rdd.getNumPartitions()

# COMMAND ----------



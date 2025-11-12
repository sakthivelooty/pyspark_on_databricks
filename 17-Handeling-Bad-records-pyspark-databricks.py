# Databricks notebook source
# MAGIC %md
# MAGIC ### Handling Malformed Data in PySpark Using PERMISSIVE, DROPMALFORMED, and FAILFAST Modes
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Students will learn how Spark handles dirty or inconsistent input data using the three modes of the mode option in file reading:
# MAGIC
# MAGIC - PERMISSIVE (default): Keeps corrupt records in a separate column.
# MAGIC
# MAGIC - DROPMALFORMED: Skips bad records.
# MAGIC
# MAGIC - FAILFAST: Stops execution if any malformed record is found.

# COMMAND ----------

df_permissive = (
    spark.read
    .option("header", True)
    .csv("/Volumes/workspace/default/external_datasets/customer_data.csv")
)

df_permissive.display()
df_permissive.printSchema()


# COMMAND ----------

df_permissive.schema

# COMMAND ----------

from pyspark.sql.types import *
user_def_schema = StructType([StructField('id', StringType(), True),
 StructField('name', StringType(), True),
 StructField('age', IntegerType(), True),
 StructField('_corrupt_record', StringType(), True)])

# COMMAND ----------

df_permissive = (
    spark.read
    .option("header", True)
    .option("mode", "PERMISSIVE")
    .option('mergeSchema', "true")
    .schema(user_def_schema)
    .csv("/Volumes/workspace/default/external_datasets/customer_data.csv")
)

df_permissive.display()
df_permissive.printSchema()


# COMMAND ----------

df_permissive = (
    spark.read
    .option("header", True)
    .option("mode", "DROPMALFORMED")
    .schema(user_def_schema)
    .csv("/Volumes/workspace/default/external_datasets/customer_data.csv")
)

df_permissive.display()
df_permissive.printSchema()


# COMMAND ----------

df_permissive = (
    spark.read
    .option("header", True)
    .option("mode", "FAILFAST")
    .schema(user_def_schema)
    .csv("/Volumes/workspace/default/external_datasets/customer_data.csv")
)

df_permissive.display()
df_permissive.printSchema()


# COMMAND ----------



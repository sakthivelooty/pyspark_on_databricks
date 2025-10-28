# Databricks notebook source
df = spark.table("samples.tpch.customer")
df_1 = df.limit(100).repartition(1)
# df_1.display()
df_1.write.mode('overwrite').format('csv').option("headers",True).save("/Volumes/workspace/default/external_datasets/sampledata_csv/")
df_1.write.mode('overwrite').format('json').option("headers",True).save("/Volumes/workspace/default/external_datasets/sampledata_json/")
df_1.write.mode('overwrite').format('parquet').option("headers",True).save("/Volumes/workspace/default/external_datasets/sampledata_parquet/")

# COMMAND ----------

df_1.schema

# COMMAND ----------

csv_file = "/Volumes/workspace/default/external_datasets/sampledata_csv.csv"
json_file = "/Volumes/workspace/default/external_datasets/sampledata_json.json"
parquet_file = "/Volumes/workspace/default/external_datasets/sampledata_parquet.parquet"

# COMMAND ----------

# DBTITLE 1,Read csv file
csv_data  = spark.read.format('csv').load(csv_file)
csv_data.display()

# COMMAND ----------

csv_data  = spark.read.json(json_file)
csv_data.display()

# COMMAND ----------

csv_data  = spark.read.parquet(parquet_file)
csv_data.display()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType, DecimalType

custom_schema = StructType([
    StructField('c_custkey', LongType(), True), 
    StructField('c_name', StringType(), True), 
    StructField('c_address', StringType(), True), 
    StructField('c_nationkey', LongType(), True), 
    StructField('c_phone', StringType(), True), 
    StructField('c_acctbal', DecimalType(18,2), True), 
    StructField('c_mktsegment', StringType(), True), 
    StructField('c_comment', StringType(), True)])

csv_data_custom_schema = spark.read.format('csv') \
    .option('header', True) \
    .schema(custom_schema) \
    .load(csv_file)

display(csv_data_custom_schema)

# COMMAND ----------



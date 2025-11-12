# Databricks notebook source
csv_file = "/Volumes/workspace/default/external_datasets/sf-fire-calls.csv"

# COMMAND ----------

# DBTITLE 1,Reading csv file with inderschema, header and seperation
df = spark.read.format('csv').option('header', True).option('inferSchema', True).option('sep',',').load(csv_file)
df.display()

# COMMAND ----------

df = spark.read.format('csv').option('header', False).option('inferSchema', True).option('sep',',').load(csv_file)
df.display()

# COMMAND ----------

df = spark.read.format('csv').option('header', True).option('inferSchema', True).load(csv_file)
df.display()

# COMMAND ----------

df = spark.read.format('csv').options(header=True, inferSchema=True, sep=',').load(csv_file)
df.display()

# COMMAND ----------

df = spark.read.csv(header=True, inferSchema=True, sep='|', path=csv_file)
df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.schema

# COMMAND ----------

csv_file_2 = "/Volumes/workspace/default/external_datasets/sampledata_csv.csv"

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
    .schema(custom_schema) \
    .load(csv_file)

csv_data_custom_schema.display()

# COMMAND ----------

# DBTITLE 1,Reading multiple files
csv_file_list = [
    '/Volumes/workspace/default/external_datasets/csv_file_example/part-00000-tid-2658311340503090957-69ac7bdd-0734-4fab-984b-2fb16125d781-213-1-c000.csv',
    
    '/Volumes/workspace/default/external_datasets/csv_file_example/part-00001-tid-2658311340503090957-69ac7bdd-0734-4fab-984b-2fb16125d781-209-1-c000.csv'
] 

df = spark.read.csv(header=True, inferSchema=True, sep=',', path=csv_file)
df.display()
df.count()

# COMMAND ----------

csv_folder = '/Volumes/workspace/default/external_datasets/csv_file_example/'
df = spark.read.csv(header=True, inferSchema=True, sep=',', path=csv_folder)
df.display()
df.count()

# COMMAND ----------



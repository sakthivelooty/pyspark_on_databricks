# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.types import *


# COMMAND ----------

# /Volumes/workspace/default/external_datasets/u.data

# COMMAND ----------

schema = StructType([
                StructField('user_id', IntegerType()),
                StructField('movie_id', IntegerType()),
                StructField('rating', IntegerType()),
                StructField('timestamp', IntegerType())
            ])
df = spark.read.csv('/Volumes/workspace/default/external_datasets/u.data', schema=schema, sep="\t")
df.display()

# COMMAND ----------

movie_DF = df.groupBy('movie_id').agg(F.count('movie_id').alias('movie_count')).orderBy(F.col('movie_count').desc())
movie_DF.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Broadcast variable

# COMMAND ----------

# MAGIC %md
# MAGIC - Broadcast a small dataset or objects to the executors so that when each partition is being transformed the objects are already there in the memeory of the executor
# MAGIC - just use sc.broadcast() to ship the objects 
# MAGIC - to use them, .value()

# COMMAND ----------

import codecs
def read_movies():
  """ reads the data file and retuens a dict of type sting, string where the key is the movie_id and the value is the movie name
  """
  movie_names = {}
  with codecs.open('/Volumes/workspace/default/external_datasets/u.item', 'r', 'ISO-8859-1', 'ignore') as f:
    movie_dict = {}
    for line in f:
      fields = line.strip().split('|')
      movie_names[int(fields[0])] = fields[1]
  return movie_names


# COMMAND ----------

# DBTITLE 1,broadcast movie names
nameDict = spark.SparkSession.broadcast(read_movies())


# COMMAND ----------

def lookup_movie_name(movie_id):
    """udf that takes the movie id and returns the movie name
    Input: Movie_id str
    output : movie name str
    """
    return nameDict.value[movie_id]

lookup_movie_name = F.udf(lookup_movie_name)


# COMMAND ----------

moviewithnames = movie_DF.withColumn('movie_name', lookup_movie_name(F.col('movie_id')))
moviewithnames.limit(10).display()

# Databricks notebook source
import pyspark.sql.functions as F
import pyspark.pandas as ps
import pandas as pd
import os

# COMMAND ----------

os.environ['PYARROW_IGNORE_TIMEZONE'] = '1'
spark.conf.set("spark.sql.ansi.enabled", "false")
spark.conf.set("spark.executorEnv.PYARROW_IGNORE_TIMEZONE", "1")

# COMMAND ----------

pandas_df = pd.DataFrame({
    "id": [1, 2, 3, 4, 5],
    "name": ["Alice", "Bob", "Charlie", "David", "Emma"],
    "age": [25, 30, 35, 40, 45]
})

# COMMAND ----------

pandas_df.display()

# COMMAND ----------

pandas_df

# COMMAND ----------

print(pandas_df)

# COMMAND ----------

# DBTITLE 1,convert pandas DF to spark DF
spark_df = spark.createDataFrame(pandas_df)

# COMMAND ----------

spark_df.printSchema()

# COMMAND ----------

# DBTITLE 1,filtering in spark on spark DF
filtered_spark_df = spark_df.filter(spark_df.age > 30)
filtered_spark_df.display()

# COMMAND ----------

# DBTITLE 1,converting spark DF back to pandas DF
converted_pandas_df = filtered_spark_df.toPandas()
converted_pandas_df.display()

# COMMAND ----------

# DBTITLE 1,pandas-on-Spark for scalable Pandas operations
ps_df = ps.DataFrame(pandas_df)

display(ps_df)

# COMMAND ----------



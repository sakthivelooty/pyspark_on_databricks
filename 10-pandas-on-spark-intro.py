# Databricks notebook source
# MAGIC %md
# MAGIC What is Pandas?
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC - Get one column: 
# MAGIC   - df['Username']
# MAGIC - Extract multiple columns:
# MAGIC   - df[['Username', 'Age']]
# MAGIC - Extract Range of rows:
# MAGIC   - df['UserName'][:5]
# MAGIC - Extract single cell:
# MAGIC   - df['Username'][5]
# MAGIC - Sort by a column:
# MAGIC   - df.sort_value(['Username'])
# MAGIC - Find data distribution:
# MAGIC   - number_of_users = df['City'].count_values()
# MAGIC - Plot
# MAGIC   - number_of_users.plot(kind = 'bar')

# COMMAND ----------

# MAGIC %md
# MAGIC https://spark.apache.org/docs/latest/api/python/getting_started/quickstart_ps.html

# COMMAND ----------

import pyspark.sql.functions as F
import pyspark.pandas as ps
import pandas as pd
import os

# COMMAND ----------

os.environ['PYARROW_IGNORE_TIMEZONE'] = '1'
spark.conf.set("spark.sql.ansi.enabled", "false")
spark.conf.set("spark.executorEnv.PYARROW_IGNORE_TIMEZONE", "1")

# COMMAND ----------

# DBTITLE 1,Creating a pandas df
p_df = ps.DataFrame({
    "id" : [1, 2, 3, 4, 5],
    "name" : ["a", "b", "c", "d", "e"],
    "age" : [10, 20, 30, 40, 50]
})

p_df

# COMMAND ----------

# DBTITLE 1,mean of age
p_df["age"].mean()

# COMMAND ----------

p_df["age_after_10_year"] = p_df["age"] * 10

# COMMAND ----------

p_df

# COMMAND ----------

# DBTITLE 1,pandas data stats
p_df.describe()

# COMMAND ----------

p_df.display()

# COMMAND ----------

filtered_ps_df = p_df[p_df["age"] > 30]
filtered_ps_df.display()
                       

# COMMAND ----------

spark_df = p_df.to_spark()
spark_df.display()

# COMMAND ----------

ps_df_from_spark = ps.DataFrame(spark_df)
ps_df_from_spark.display()

# COMMAND ----------



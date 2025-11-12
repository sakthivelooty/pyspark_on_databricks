# Databricks notebook source
# MAGIC %md
# MAGIC ## Joins
# MAGIC - A join in PySpark combines rows from two DataFrames based on a common key (or condition).
# MAGIC It’s similar to SQL joins — you can use join() or SQL-style syntax.
# MAGIC
# MAGIC ### Basic Syntax
# MAGIC   - df_joined = df1.join(df2, on="id", how="inner")
# MAGIC
# MAGIC   - https://stackoverflow.com/questions/53949197/isnt-sql-a-left-join-b-just-a
# MAGIC   - https://blog.codinghorror.com/a-visual-explanation-of-sql-joins/

# COMMAND ----------

# DBTITLE 1,creating the dataframe
data1 = [
    (1, "Pirate"),
    (2, "Monkey"),
    (3, "Ninja"),
    (4, "Spaghetti"),
    (None, "Unknown_Left")   # Null id in left
]

data2 = [
    (1, "Rutabaga"),
    (2, "Pirate"),
    (3, "Darth Vader"),
    (None, "Unknown_Right"),  # Null id in right
    (5, "Knight")
]

# COMMAND ----------

df1 = spark.createDataFrame(data1, ["id", "name"])
df2 = spark.createDataFrame(data2, ["id", "name"])

print("=== Table 1 ===")
df1.display()

print("=== Table 2 ===")
df2.display()

# COMMAND ----------

df1 = df1.withColumnRenamed("name", "name1")
df2 = df2.withColumnRenamed("name", "name2")

# COMMAND ----------

# DBTITLE 1,INNER JOIN - Keeps only rows with matching 'id' in both DataFrames.

inner_df = df1.join(df2, on="['id','name']", how="inner")
inner_df.display()

# COMMAND ----------

# DBTITLE 1,LEFT JOIN - Keeps all rows from LEFT (df1), fills unmatched RIGHT columns with null
left_df = df1.join(df2, on="id", how="left")
left_df.display()

# COMMAND ----------

# DBTITLE 1,RIGHT JOIN - Keeps all rows from RIGHT (df2), fills unmatched LEFT columns with null
right_df = df1.join(df2, on="id", how="right")
right_df.display()

# COMMAND ----------

# DBTITLE 1,FULL OUTER JOIN (same as OUTER)  - Keeps all rows from both DataFrames, fills nulls when no match
full_outer_df = df1.join(df2, on="id", how="full_outer")
full_outer_df.display()

# COMMAND ----------

# DBTITLE 1,LEFT OUTER JOIN - Same as Left join, all the rows from the Left table and matching or nulls from right
left_outer_df = df1.join(df2, on="id", how="left_outer")
left_outer_df.display()

# COMMAND ----------

# DBTITLE 1,RIGHT OUTER - Same as RIGHT join — all rows from right with matches or nulls
right_outer_df = df1.join(df2, on="id", how="right_outer")
right_outer_df.display()

# COMMAND ----------

# DBTITLE 1,LEFT ANTI  - Returns rows from LEFT that have NO matching 'id' in RIGHT
left_anti_df = df1.join(df2, on="id", how="left_anti")
left_anti_df.display()

# COMMAND ----------

# DBTITLE 1,LEFT SEMI JOIN - Returns rows from LEFT that HAVE a match in RIGHT (no right columns).
left_semi_df = df1.join(df2, on="id", how="left_semi")
left_semi_df.display()

# COMMAND ----------

# DBTITLE 1,CROSS JOIN - Cartesian product: every row in df1 joins with every row in df2
cross_df = df1.crossJoin(df2)
cross_df.display()

# COMMAND ----------

# DBTITLE 1,NULL-SAFE JOIN (eqNullSafe)
# MAGIC %md
# MAGIC ##### Normal join condition ignores null == null, so we use eqNullSafe() or <=> operator.
# MAGIC - NULL = NULL        →  UNKNOWN  (not true)
# MAGIC - NULL != NULL       →  UNKNOWN  (also not true)
# MAGIC
# MAGIC ### Issues with NULL's in the data
# MAGIC - Unexpected missing joins — rows you thought should match (both have NULLs) won’t.
# MAGIC
# MAGIC - Data loss — especially in INNER joins.
# MAGIC
# MAGIC - Duplicate null rows — in OUTER joins.
# MAGIC
# MAGIC - Wrong metrics — counts, aggregations, or mappings may undercount records.

# COMMAND ----------

df_nullsafe = df1.join(df2, on=(df1["id"].eqNullSafe(df2["id"])), how="left")
df_nullsafe.display()

# COMMAND ----------

inner_df.display()

# COMMAND ----------



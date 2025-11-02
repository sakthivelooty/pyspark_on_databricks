# Databricks notebook source
# MAGIC %md
# MAGIC Objective
# MAGIC
# MAGIC - Understand how to pivot data — turning rows into columns.
# MAGIC
# MAGIC - Understand how to unpivot (melt) data — turning columns back into rows.
# MAGIC
# MAGIC - Learn real-world analytical use cases using Sales / Orders data inspired by TPC-H.

# COMMAND ----------

# MAGIC %md
# MAGIC In analytics, data often needs reshaping:
# MAGIC
# MAGIC - Pivot: summarize data and transform unique row values into columns.
# MAGIC
# MAGIC - Unpivot (melt): perform the reverse, converting multiple columns into key–value pairs for easier analysis or aggregation.

# COMMAND ----------

data = [
    ("North", "Q1", 12000),
    ("North", "Q2", 15000),
    ("South", "Q1", 10000),
    ("South", "Q2", 9000),
    ("East", "Q1", 8000),
    ("East", "Q2", 9500),
]

sales_df = spark.createDataFrame(data, ["region", "quarter", "revenue"])

sales_df.display()


# COMMAND ----------

# DBTITLE 1,Pivot
pivot_df = (
    sales_df
    .groupBy("region")
    .pivot("quarter")     # column whose values become new column names
    .sum("revenue")       # aggregation function
)

pivot_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC stack(n, col_name1, value1, col_name2, value2, …)
# MAGIC
# MAGIC   - n = number of columns to unpivot.
# MAGIC
# MAGIC   - Each pair ('Q1', Q1) creates one row with key–value pairs.
# MAGIC
# MAGIC   - The output aliases define the new columns (here: quarter, revenue).

# COMMAND ----------

# DBTITLE 1,Unpivot
unpivot_df = (
    pivot_df
    .selectExpr(
        "region",
        "stack(2, 'Q1', Q1, 'Q2', Q2) as (quarter, revenue)"
    )
)

unpivot_df.display()


# COMMAND ----------

data2 = [
    ("North", "Q1", 12000),
    ("North", "Q2", 15000),
    ("South", "Q1", 10000),
    ("East", "Q2", 9500),
]

sales_df2 = spark.createDataFrame(data2, ["region", "quarter", "revenue"])

sales_df2.groupBy("region").pivot("quarter").sum("revenue").display()


# COMMAND ----------



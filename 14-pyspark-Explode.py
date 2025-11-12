# Databricks notebook source
# MAGIC %md
# MAGIC ## Explode
# MAGIC https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.explode.html

# COMMAND ----------

# MAGIC %md
# MAGIC - when arrrys or list are passed into this function, it creates a new row for each element in array. when a map is passed, it creates tow new columns for one key and value and each key value pair splits into a new row.
# MAGIC
# MAGIC ##### variants
# MAGIC - Explode
# MAGIC - Explode_outer
# MAGIC - posexplode
# MAGIC - posexplode_outer
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC | Function             | Keeps Null Rows | Adds Position Index | Description                            |
# MAGIC | -------------------- | --------------- | ------------------- | -------------------------------------- |
# MAGIC | `explode()`          | ❌               | ❌                   | Flattens array; drops nulls            |
# MAGIC | `posexplode()`       | ❌               | ✅                   | Flattens array with position           |
# MAGIC | `explode_outer()`    | ✅               | ❌                   | Flattens array; keeps nulls            |
# MAGIC | `posexplode_outer()` | ✅               | ✅                   | Flattens array with position and nulls |
# MAGIC

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

data = [
    (1, "CustomerA", [
        {"partkey": 101, "qty": 5, "price": 100},
        {"partkey": 102, "qty": 2, "price": 250}
    ]),
    (2, "CustomerB", [
        {"partkey": 103, "qty": 1, "price": 500}
    ]),
    (3, "CustomerC", None)  # Missing line items
]

orders_df = spark.createDataFrame(data, ["order_id", "customer_name", "line_items"])

orders_df.display()
orders_df.printSchema()


# COMMAND ----------

# DBTITLE 1,explode
df_explode = orders_df.select(
    "order_id",
    "customer_name",
    F.explode("line_items").alias("item")
)
df_explode.display()


# COMMAND ----------

# DBTITLE 1,select exploded columns

df_explode.select(
    "order_id",
    "customer_name",
    F.col("item.partkey").alias("partkey"),
    F.col("item.qty").alias("quantity"),
    F.col("item.price").alias("price")
).display()

# COMMAND ----------

# DBTITLE 1,posexplode
df_posexplode = orders_df.select(
    "order_id",
    "customer_name",
    F.posexplode("line_items").alias("pos", "item")
)
df_posexplode.display()



# COMMAND ----------

df_posexplode.select(
    "order_id",
    "pos",
    F.col("item.partkey").alias("partkey"),
    F.col("item.qty").alias("quantity"),
    F.col("item.price").alias("price")
).display()

# COMMAND ----------

# DBTITLE 1,explode_outer
df_explode_outer = orders_df.select(
    "order_id",
    "customer_name",
    F.explode_outer("line_items").alias("item")
)
df_explode_outer.display()


# COMMAND ----------

df_explode_outer.select(
    "order_id",
    "customer_name",
    F.col("item.partkey").alias("partkey"),
    F.col("item.qty").alias("quantity"),
    F.col("item.price").alias("price")
).display()


# COMMAND ----------

# DBTITLE 1,posexplode_outer
df_posexplode_outer = orders_df.select(
    "order_id",
    "customer_name",
    F.posexplode_outer("line_items").alias("pos", "item")
)
df_posexplode_outer.display()



# COMMAND ----------


df_posexplode_outer.select(
    "order_id",
    "pos",
    "customer_name",
    F.col("item.partkey").alias("partkey"),
    F.col("item.qty").alias("quantity"),
    F.col("item.price").alias("price")
).display()

# COMMAND ----------

data = [
    (1, ["red", "blue", "green"]),
    (2, ["yellow"]),
    (3, []),
    (4, None)
]

df = spark.createDataFrame(data, ["id", "colors"])

# COMMAND ----------

df_exploded = df.select(df.id, F.explode(df.colors).alias("color"))
df_exploded.display()


# COMMAND ----------



df_outer = df.select(df.id, F.explode_outer(df.colors).alias("color"))
df_outer.display()


# COMMAND ----------



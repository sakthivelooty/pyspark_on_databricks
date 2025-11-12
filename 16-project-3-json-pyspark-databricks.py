# Databricks notebook source
# MAGIC %md
# MAGIC Below is a full mini-project that uses your dataset and integrates these functions:
# MAGIC - `explode, explode_outer`
# MAGIC - `union`
# MAGIC - `when (CASE WHEN equivalent)`
# MAGIC - `pivot`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Project Title: Analyzing Coffee Brewing Data from Nested JSON
# MAGIC
# MAGIC #### Objective
# MAGIC
# MAGIC - Read and flatten a nested JSON dataset.
# MAGIC - Use explode and explode_outer to handle arrays with and without nulls.
# MAGIC - Combine two DataFrames using union.
# MAGIC - Apply conditional logic using when.
# MAGIC - Reshape the data using pivot.

# COMMAND ----------


from pyspark.sql.functions import explode, explode_outer, col, when



df = spark.read.option("multiline", True).json("/Volumes/workspace/default/external_datasets/coffee_data.json")
df.printSchema()
df.display()


# COMMAND ----------

df.select(col("coffee.region").getItem(0).getField("name")).display()

# COMMAND ----------

# DBTITLE 1,Coffee Regions
# Using explode: ignores nulls
coffee_df = df.select(explode("coffee.region").alias("region"), col("coffee.country.company").alias("company"))
coffee_df.display()

# COMMAND ----------

# DBTITLE 1,Brewing Regions
brewing_df = df.select(explode_outer("brewing.region").alias("region"), col("brewing.country.company").alias("company"))
brewing_df.display()

# COMMAND ----------

# DBTITLE 1,Combine Coffee and Brewing Data using UNION
combined_df = coffee_df.union(brewing_df)
combined_df.display()

# COMMAND ----------

from pyspark.sql.functions import lit

enriched_df = combined_df.withColumn(
    "company_type",
    when(col("company") == "ACME", lit("Local"))
    .when(col("company") == "BrewCo", lit("Partner"))
    .otherwise(lit("International"))
)
enriched_df.display()


# COMMAND ----------

# DBTITLE 1,Pivot to Compare Employee Counts per Company
from pyspark.sql.functions import count

pivot_df = (
    enriched_df.groupBy("company")
    .pivot("region.name")
    .agg(count("company"))
)

pivot_df.display()


# COMMAND ----------

df.schema.fields

# COMMAND ----------

def flatten(df):
    """
    Recursively flattens all StructType and ArrayType columns in a DataFrame.
    StructType columns are expanded into individual columns.
    ArrayType columns are exploded into multiple rows.
    """

    from pyspark.sql.types import StructType, ArrayType
    from pyspark.sql.functions import col, explode_outer

    complex_fields = dict([
        (field.name, field.dataType)
        for field in df.schema.fields
        if isinstance(field.dataType, (ArrayType, StructType))
    ])

    while len(complex_fields) != 0:
        col_name = list(complex_fields.keys())[0]
        print(f"Processing: {col_name}, Type: {type(complex_fields[col_name])}")

        # Flatten StructType
        if isinstance(complex_fields[col_name], StructType):
            expanded = [
                col(f"{col_name}.{k.name}").alias(f"{col_name}_{k.name}")
                for k in complex_fields[col_name]
            ]
            df = df.select("*", *expanded).drop(col_name)

        # Explode ArrayType
        elif isinstance(complex_fields[col_name], ArrayType):
            df = df.withColumn(col_name, explode_outer(col_name))

        # Recompute remaining complex fields
        complex_fields = dict([
            (field.name, field.dataType)
            for field in df.schema.fields
            if isinstance(field.dataType, (ArrayType, StructType))
        ])

    return df


# COMMAND ----------

flat_df = flatten(df)
flat_df.printSchema()
flat_df.display()


# COMMAND ----------

# DBTITLE 1,Challenge for Students
# MAGIC %md
# MAGIC - Add a `source` column (“coffee” or “brewing”) before doing the union.
# MAGIC - Rorder the columns before union
# MAGIC - Write one query showing:
# MAGIC   - Total number of people per company.
# MAGIC   - Number of people unique to each company.
# MAGIC - Experiment replacing `explode` with `explode_outer/posexplode/posexplode-outer` and explain the difference.

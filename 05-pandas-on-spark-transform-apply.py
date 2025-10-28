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

ps_df = ps.DataFrame({
    "id": [1, 2, 3, 4, 5],
    "name": ["Alice", "Bob", "Charlie", "David", "Emma"],
    "age": [25, 30, 35, 40, 45],
    "salary": [50000, 60000, 75000, 80000, 120000]
})


# COMMAND ----------

ps_df.display()

# COMMAND ----------

# DBTITLE 1,1. Using `transform()` for element-wise operations
ps_df["age_in_10_years"] = ps_df["age"].transform(lambda x: x + 10)

# COMMAND ----------

ps_df.display()

# COMMAND ----------

def categorize_salary(salary):
    if salary < 60000:
        return "Low"
    elif salary < 100000:
        return "Medium"
    else:
        return "High"

# Apply the function to the 'salary' column using transform
ps_df["salary_category"] = ps_df["salary"].transform(categorize_salary)

# COMMAND ----------

ps_df.display()

# COMMAND ----------

# DBTITLE 1,Using `apply()` on columns
def categorize_salary(salary):
    if salary < 60000:
        return "Low"
    elif salary < 100000:
        return "Medium"
    else:
        return "High"

# Apply the function to the 'salary' column
ps_df["salary_category"] = ps_df["salary"].apply(categorize_salary)

# COMMAND ----------

ps_df.display()

# COMMAND ----------

spark.conf.set('compute.ops_on_diff_frames', True)

# COMMAND ----------

# DBTITLE 1,3. Using `apply()` on rows
def format_row(row):
    return f"{row['name']} ({row['age']} years old)"

# Apply the function across rows
ps_df["name_with_age"] = ps_df.apply(format_row, axis=1)

# COMMAND ----------

ps_df.display()

# COMMAND ----------

df = ps.DataFrame({
    'dept': ['IT', 'IT', 'HR', 'HR', 'Finance', 'Finance'],
    'emp': ['Alice', 'Bob', 'Charlie', 'David', 'Eve', 'Frank'],
    'salary': [100, 150, 200, 250, 300, 350],
    'bonus': [10, 20, 30, 40, 50, 60]
})

# COMMAND ----------

def summarize(group):
    avg_salary = group['salary'].mean()
    total_bonus = group['bonus'].sum()
    return ps.Series({
        'avg_salary': avg_salary,
        'total_bonus': total_bonus,
        'num_employees': len(group)
    })

result = df.groupby('dept').apply(summarize).reset_index()
print(result)

# COMMAND ----------

df.groupby('dept').agg(
    avg_salary=('salary', 'mean'),
    total_bonus=('bonus', 'sum'),
    num_employees=('emp', 'count')
)

# COMMAND ----------

df['avg_salary_by_dept'] = df.groupby('dept')['salary'].transform('mean')
df['total_bonus_by_dept'] = df.groupby('dept')['bonus'].transform('sum')

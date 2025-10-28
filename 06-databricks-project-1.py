# Databricks notebook source
# DBTITLE 1,import required lib
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# COMMAND ----------

customer = spark.table('samples.bakehouse.sales_customers')

# COMMAND ----------

customer.show()

# COMMAND ----------

customer_schema

# COMMAND ----------

# DBTITLE 1,read customers tables



customer = customer.withColumn('full_name', F.concat_ws(' ', F.col('first_name'), F.col('last_name')))

# COMMAND ----------

customer.display()

# COMMAND ----------

# DBTITLE 1,Read transaction table
transaction = spark.table('samples.bakehouse.sales_transactions')

# COMMAND ----------

transaction.select('customerID').distinct().count()

# COMMAND ----------

customer.select("customerID").distinct().count()

# COMMAND ----------

customer.display()

# COMMAND ----------

# DBTITLE 1,Join customer and transaction
customer_transaction = customer.join(
    transaction,
    F.substring(customer.customerID.cast("string"), -3, 3) == F.substring(transaction.customerID.cast("string"), -3, 3),
    'inner'
)

# COMMAND ----------

customer_transaction.display()

# COMMAND ----------

transaction.select('customerID').distinct().orderBy('customerID').display()

# COMMAND ----------

customer.select('customerID').distinct().orderBy('customerID').display()

# COMMAND ----------

# DBTITLE 1,clean dataframe and remove duplicate columns
customer_transaction_clean = customer_transaction.drop(transaction.customerID)
display(customer_transaction_clean)

# COMMAND ----------

# DBTITLE 1,Group and Agg function
from pyspark.sql.functions import count, avg

avg_orders = customer_transaction_clean.groupBy('customerID', 'full_name').agg(count('*').alias('orders'))

display(avg_orders.orderBy('orders'))

# COMMAND ----------

# DBTITLE 1,Customers who had the min sales and max sales


w = Window.orderBy(F.desc('orders'))
max_orders = avg_orders.withColumn('rank', F.rank().over(w)).filter(F.col('rank') == 1).drop('rank')

w_asc = Window.orderBy('orders')
min_orders = avg_orders.withColumn('rank', F.rank().over(w_asc)).filter(F.col('rank') == 1).drop('rank')

display(max_orders)
display(min_orders)

# COMMAND ----------

# DBTITLE 1,Top 10 Customers who had the most sales
revenue_per_customer = customer_transaction_clean.groupBy('customerID', 'Full_name').agg(
    F.sum('totalPrice').alias('total_revenue')
)

w_revenue = Window.orderBy(F.desc('total_revenue'))
top_customers = revenue_per_customer.withColumn('rank', F.rank().over(w_revenue)).filter(F.col('rank') <= 10).drop('rank')

display(top_customers)

# COMMAND ----------



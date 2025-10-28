# Databricks notebook source
# MAGIC %md
# MAGIC User defined Table Function & User defined Functions
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC user-Defined Functions (UDFs) allow custom logic to be applied Row by Row in Spark SQL & Dataframe API
# MAGIC - One row at a time!!
# MAGIC - Each row must be serialized / Deserialized 
# MAGIC
# MAGIC Example : 

# COMMAND ----------

# from pyspark.sql.functions import udf
# from pyspark.sql.types import StringType

# @udf(returnType = StringType())
# def to_uppercase(text):
#   return text.upper()

# #apply to Dataframe 
# df = df.withColumn("upper", to_uppercase(df["name"]))

# COMMAND ----------

# MAGIC %md
# MAGIC UDTF - User defined Table Function
# MAGIC - One input row can retuen multiple output rows and columns
# MAGIC - Useful for trasforming nested or Structural data
# MAGIC   - Expanding JSON, arrays, hierarchical data
# MAGIC  

# COMMAND ----------

# from pyspark.sql.functions import udtf
# from pyspark.sql.types import StringType

# @udtf(returnType = "user_id String, event String")
# def parse_event(json_str: str) -> Iterator[Tuple[str, str]]:
#   data = json.loads(json_str)
#   for event in data["events"]:
#       user_id = data.get("user", "unknowsuser")
#       for wvent in data.get('events')
#         yield (user_id, event)


# df = spark.sql("select parse_event(json_str) ")
# df.show()

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import udtf, udf
from pyspark.sql.types import IntegerType
import re

# COMMAND ----------

spark.conf.set('spark.sql.execution.pythonUDTF.enable', True)

# COMMAND ----------

@udtf(returnType="hashtag: string")
class HashtagExtractor:
    def eval(self, text: str):
        """Extracts hashtags from the input text."""
        if text:
            hashtags = re.findall(r"#\w+", text)
            for hashtag in hashtags:
                yield (hashtag,)

# COMMAND ----------

@udf(returnType=IntegerType())
def count_hashtags(text: str):
    """Counts the number of hashtags in the input text."""
    if text:
        return len(re.findall(r"#\w+", text))
    return 0

# COMMAND ----------

# Register the UDTF for use in Spark SQL
spark.udtf.register("extract_hashtags", HashtagExtractor)

# Register the UDF for use in Spark SQL
spark.udf.register("count_hashtags", count_hashtags)

# COMMAND ----------

spark.sql("SELECT * FROM extract_hashtags('Welcome to #ApacheSpark and #BigData!')").display()

# COMMAND ----------

spark.sql("SELECT count_hashtags('Welcome to #ApacheSpark and #BigData!') AS hashtag_count").show()

# COMMAND ----------

data = [("Learning #AI with #ML",), ("Explore #DataScience",), ("No hashtags here",)]
df = spark.createDataFrame(data, ["text"])


# COMMAND ----------

df.selectExpr("text", "count_hashtags(text) AS num_hashtags").show()

# COMMAND ----------

df.createOrReplaceTempView("tweets")
spark.sql(
    "SELECT text, hashtag FROM tweets, LATERAL extract_hashtags(text)"
).show()

# COMMAND ----------



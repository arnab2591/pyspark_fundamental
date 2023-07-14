# Databricks notebook source
from pyspark.sql import dataframe

# COMMAND ----------

help(dataframe)

# COMMAND ----------

help(dataframe.DataFrame.write)

# COMMAND ----------

df = spark.createDataFrame([("Bob", 13, 40.3, 150.5), ("Alice", 12, 37.8, 142.3), ("Tom", 11, 44.1, 142.2)],["name", "age", "weight", "height"])
display(df)

# COMMAND ----------

type(df.write)

# COMMAND ----------

help(df.write)

# COMMAND ----------

df.write.csv(path='dbfs:/tmp/emps',header=True)

# COMMAND ----------

display(spark.read.csv(path='dbfs:/tmp/emps',header=True))

# COMMAND ----------


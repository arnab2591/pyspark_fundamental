# Databricks notebook source
[("Bob", 13, 40.3, 150.5), ("Alice", 12, 37.8, 142.3), ("Tom", 11, 44.1, 142.2)]

# COMMAND ----------

data = [(1,'Arnab'),(2,'Wafa')]
schema = ['id','name']
df= spark.createDataFrame(data=data,schema=schema)
display(df)

# COMMAND ----------

help(df.write)

# COMMAND ----------

df.write.json('dbfs:/FileStore/jsondata/emp',mode='append')

# COMMAND ----------

display(spark.read.json('dbfs:/FileStore/jsondata/emp'))

# COMMAND ----------


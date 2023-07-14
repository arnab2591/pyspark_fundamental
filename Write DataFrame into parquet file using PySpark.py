# Databricks notebook source
data = [(1,'Arnab'),(2,'Wafa')]
schema = ['id','name']
df= spark.createDataFrame(data=data,schema=schema)
display(df)

# COMMAND ----------

help(df.write.parquet)

# COMMAND ----------

df.write.parquet('dbfs:/FileStore/parquetdata/')

# COMMAND ----------

display(spark.read.parquet('dbfs:/FileStore/parquetdata/'))

# COMMAND ----------


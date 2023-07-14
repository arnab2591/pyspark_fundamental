# Databricks notebook source
from pyspark.sql.functions import when

data = [(1,'Maheer','M',2000),(2,'Asi','F',3000),(2,'Asi','',3000)]
columns = ['id','name','gender','salary']

df = spark.createDataFrame(data=data,schema=columns)
df.show()

# COMMAND ----------

from pyspark.sql.functions import when

help(when)


# COMMAND ----------

df1 = df.select(df.id,df.name,df.gender,when(df.gender=='M','male').when(df.gender=='F','female').otherwise('unknown').alias('gender2'))
df1.show()

# COMMAND ----------


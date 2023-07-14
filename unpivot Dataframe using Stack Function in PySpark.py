# Databricks notebook source
data = [('IT',8,5),('Payroll',3,2),('HR',2,4)]

schema = ['Dept','Male','female']

df= spark.createDataFrame(data=data,schema=schema)
df.show()

# COMMAND ----------

from pyspark.sql.functions import expr

unpivot_df = df.select('Dept',expr("stack(2,'male',Male,'female',female) as (gender,count)"))

unpivot_df.show()

# COMMAND ----------


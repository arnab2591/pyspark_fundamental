# Databricks notebook source
data = [('maheer','{"hair":"black","eye":"brown"}'),('Arnab','{"hair":"black","eye":"black"}')]

schema = ['name','props']

df = spark.createDataFrame(data, schema)
df.show(truncate=False)
df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import json_tuple
help(json_tuple)

# COMMAND ----------

df1 = df.select(df.name,json_tuple(df.props,'hair','eye').alias('hair','eye'))
df1.show(truncate=False)

# COMMAND ----------

df2 = df.withColumn("hair",json_tuple(df.props,'hair')).withColumn("eye",json_tuple(df.props,'eye'))
df2.show(truncate=False)

# COMMAND ----------

df3 = df.withColumn("hair",json_tuple(df.props,'hair')).withColumn("eye",json_tuple(df.props,'eye')).select('name','hair','eye')
df3.show(truncate=False)

# COMMAND ----------


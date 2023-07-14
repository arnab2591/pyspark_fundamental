# Databricks notebook source
data = [('maheer','{"address":{"city":"hyderabad","state":"telengana"},"gender":"male"}'),
        ('Arnab','{"address":{"city":"bangalore","state":"Karnataka"},"gender":"male"}')]

schema = ['name','props']

df = spark.createDataFrame(data, schema)
df.show(truncate=False)
df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import get_json_object

help(get_json_object)

# COMMAND ----------

df1 = df.select('name', get_json_object('props','$.address.city').alias('city'), get_json_object('props','$.address.state').alias('state'))
df1.show(truncate=False)
df1.printSchema()

# COMMAND ----------

df2 = df.withColumn('city',get_json_object('props','$.address.city'))
df2.show(truncate=False)

# COMMAND ----------


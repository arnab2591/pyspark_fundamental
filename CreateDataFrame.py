# Databricks notebook source
type(spark)

# COMMAND ----------

dir(spark)

# COMMAND ----------

help(spark.createDataFrame)

# COMMAND ----------

data = [(1,'Arnab'),(2,'Maheer')]

df = spark.createDataFrame(data=data,schema=['id','name'])
df.show()
df.printSchema()

# COMMAND ----------

from pyspark.sql.types import *
data = [(1,'Arnab'),(2,'Maheer')]

schema=StructType([StructField(name='id',dataType=IntegerType()),
           StructField(name='name',dataType=StringType())])

df = spark.createDataFrame(data=data,schema=schema)
df.show()
df.printSchema()

# COMMAND ----------

from pyspark.sql.types import *
help(StructType)

# COMMAND ----------

schema=StructType([StructField(name='id',dataType=IntegerType()),
           StructField(name='name',dataType=StringType())])
type(schema)

# COMMAND ----------

data = [{'id':1,'name':'maheer'},
       {'id':2,'name':'wafa'}]

df = spark.createDataFrame(data=data)
df.show()
df.printSchema()

# COMMAND ----------

import json

# COMMAND ----------


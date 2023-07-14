# Databricks notebook source
help(spark.read)

# COMMAND ----------

help(spark.read.json)

# COMMAND ----------

df = spark.read.json(path="/FileStore/data/emp-1.json")
df.printSchema()
df.show()

# COMMAND ----------

df = spark.read.json(path="/FileStore/data/emp_ML.json")
df.printSchema()
df.show()

# COMMAND ----------

df = spark.read.json(path="/FileStore/data/emp_ML.json",multiLine=True)
df.printSchema()
df.show()

# COMMAND ----------

from pyspark.sql.types import *

schema = StructType().add(field='id',data_type=IntegerType())\
                     .add(field='name',data_type=StringType())\
                     .add(field='gender',data_type=StringType())\
                     .add(field='salary',data_type=IntegerType())

# COMMAND ----------

df = spark.read.json(path=["/FileStore/data/emp-1.json","/FileStore/data/emp-2.json"],schema=schema)
df.printSchema()
df.show()

# COMMAND ----------

df = spark.read.json(path="/FileStore/data/*.json",multiLine=True)
df.printSchema()
df.show()

# COMMAND ----------


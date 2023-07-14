# Databricks notebook source
help(spark)

# COMMAND ----------

help(spark.read)

# COMMAND ----------

# DBTITLE 1,Read csv file method 1
df=spark.read.csv(path='dbfs:/FileStore/data/Emp1.csv',header=True)
display(df)
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Read csv file method 2
df = spark.read.format("csv").option(key='header',value=True).load('dbfs:/FileStore/data/Emp1.csv')
display(df)
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Loading data from different folder with same csv file structure
df=spark.read.csv(path=['dbfs:/FileStore/data/Emp1.csv','dbfs:/FileStore/data1/Emp2.csv'],header=True)
display(df)
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Reading all csv files from one folder
df=spark.read.csv(path='dbfs:/FileStore/data/',header=True)
display(df)
df.printSchema()df=spark.read.csv(path='dbfs:/FileStore/data/',header=True)
display(df)
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Reading csv file with custom schema
from pyspark.sql.types import *

schema=StructType().add(field='Id',data_type=IntegerType())\
                   .add(field='Name',data_type=StringType())\
                   .add(field='Gender',data_type=StringType())\
                   .add(field='Salary',data_type=IntegerType())
           
df=spark.read.csv(path='dbfs:/FileStore/data/',schema=schema,header=True)
display(df)
df.printSchema()


# COMMAND ----------

help(StructType)

# COMMAND ----------


# Databricks notebook source
help(spark.read.parquet)

# COMMAND ----------

# DBTITLE 1,Reading a single parquet file
df = spark.read.parquet('dbfs:/FileStore/data/userdata1.parquet')
display(df)
print(df.count())

# COMMAND ----------

# DBTITLE 1,Reading multiple Parquet file
df = spark.read.parquet('dbfs:/FileStore/data/*.parquet')
display(df)
print(df.count())

# COMMAND ----------

# DBTITLE 1,Reading multiple Parquet file in a specific folder
df = spark.read.parquet('dbfs:/FileStore/data/parquetdata/')
display(df)
print('Count of rows in dataframe is - ' + str(df.count()))

# COMMAND ----------


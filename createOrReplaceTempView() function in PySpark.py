# Databricks notebook source
data = [(1,'maheer',2000),(2,'Arnab',4000)]

schema = ['id','name','salary']

df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

df.select('id','name').show()

# COMMAND ----------

help(df.createOrReplaceTempView)

# COMMAND ----------

df.createOrReplaceTempView('employees')

df1 = spark.sql("SELECT * from employees")
df1.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT id,upper(name) as name from employees

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC Select * from global_temp.EmpGlobal

# COMMAND ----------

spark.catalog.listTables('global_temp')

# COMMAND ----------


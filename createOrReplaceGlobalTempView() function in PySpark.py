# Databricks notebook source
# MAGIC %scala
# MAGIC spark

# COMMAND ----------

data = [(1,'maheer',2000),(2,'Arnab',4000)]

schema = ['id','name','salary']

df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

# DBTITLE 1,Defining Global table
df.createOrReplaceGlobalTempView('EmpGlobal')

# COMMAND ----------

# DBTITLE 1,Quering to global Table
# MAGIC %sql
# MAGIC
# MAGIC Select * from global_temp.EmpGlobal

# COMMAND ----------

spark.catalog.currentDatabase()

# COMMAND ----------

spark.catalog.listTables('default')

# COMMAND ----------

spark.catalog.listTables('global_temp')

# COMMAND ----------

spark.catalog.listTables('default')

# COMMAND ----------

spark.catalog.dropGlobalTempView('')
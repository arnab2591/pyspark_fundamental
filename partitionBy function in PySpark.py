# Databricks notebook source
data = [(1,'maheer','male','IT'),(2,'wafa','male','HR'),(3,'asi','female','IT')]

schema = ['id','name','gender','dep']

df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

help(df.write.parquet)

# COMMAND ----------

df.write.parquet('/FileStore/Employees',mode='overwrite',partitionBy='dep')

# COMMAND ----------

spark.read.parquet('/FileStore/Employees').show()

# COMMAND ----------

spark.read.parquet('/FileStore/Employees/dep=IT').show()

# COMMAND ----------

spark.read.parquet('/FileStore/Employees/dep=HR').show()

# COMMAND ----------

df.write.parquet('/FileStore/Employees1',mode='overwrite',partitionBy=['dep','gender'])

# COMMAND ----------

spark.read.parquet('/FileStore/Employees1/dep=IT/gender=female').show()

# COMMAND ----------


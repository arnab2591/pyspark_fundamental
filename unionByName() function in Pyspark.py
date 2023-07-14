# Databricks notebook source
data1 = [(1,'Arnab','M'),(2,'Wafa','F')]
schema1 = ['id','name','gender']
df1= spark.createDataFrame(data=data1,schema=schema1)
df1.show()

# COMMAND ----------

data2 = [(3,'Asi',5000,),(4,'Ayesha',6000)]

schema2 = ['id','name','salary']
df2= spark.createDataFrame(data=data2,schema=schema2)
df2.show()

# COMMAND ----------

df1.union(df2).show()

# COMMAND ----------

df1.unionByName(df2).show()

# COMMAND ----------

df1.unionByName(df2,allowMissingColumns=True).show()

# COMMAND ----------


# Databricks notebook source
data = [(1,'Arnab',2000),(2,'Wafa',4000),(3,'Asi',5000)]
schema = ['id','name','salary']
df1= spark.createDataFrame(data=data,schema=schema)
df2= spark.createDataFrame(data=data,schema=schema)
df1.show()
df2.show()

# COMMAND ----------

new_df = df1.union(df2)
new_df.show()

# COMMAND ----------

new_df = df1.unionAll(df2)
new_df.show()

# COMMAND ----------

new_df.distinct().show()

# COMMAND ----------


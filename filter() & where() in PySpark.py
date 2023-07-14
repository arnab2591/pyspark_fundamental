# Databricks notebook source
data = [(1,'Arnab','male',2000),(2,'Wafa','male',4000),(3,'Ari','female',5000)]
schema = ['id','name','gender','salary']
df= spark.createDataFrame(data=data,schema=schema)
display(df)

# COMMAND ----------

help(df.filter)

# COMMAND ----------

# DBTITLE 1,Filter
df.filter(df.gender=='male').show()

# COMMAND ----------

df.filter("gender == 'male'").show()

# COMMAND ----------

# DBTITLE 1,Where Command
df.where(df.gender == 'male').show()

# COMMAND ----------

df.where((df.gender == 'male') & (df.salary > 2000)).show()

# COMMAND ----------


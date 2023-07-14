# Databricks notebook source
data = [(1,'Arnab',2000),(2,'Wafa',4000),(2,'Wafa',4000),(3,'Asi',5000)]
schema = ['id','name','salary']
df= spark.createDataFrame(data=data,schema=schema)
display(df)

# COMMAND ----------

help(df.distinct)

# COMMAND ----------

df.distinct().show()

# COMMAND ----------

help(df.dropDuplicates)

# COMMAND ----------

df.dropDuplicates().show()

# COMMAND ----------

df.dropDuplicates(['salary','id']).show()

# COMMAND ----------

help(df.sort)

# COMMAND ----------

df.sort('salary').show()
df.sort('name','salary').show()

# COMMAND ----------

df.orderBy('salary').show()

# COMMAND ----------

df.orderBy(df.salary.desc()).show()

# COMMAND ----------


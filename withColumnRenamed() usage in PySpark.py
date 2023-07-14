# Databricks notebook source
data = [(1,'Arnab',2000),(2,'Wafa',3000)]
schema = ['id','name','salary']
df= spark.createDataFrame(data=data,schema=schema)
display(df)

# COMMAND ----------

df.show()

# COMMAND ----------

display(df.withColumnRenamed('salary','salary_amount'))

# COMMAND ----------

df1 = df.withColumnRenamed('salary','salary_amount')
df1.show() 

# COMMAND ----------


# Databricks notebook source
data = [(1,'Arnab',2000),(2,'Wafa',4000)]
schema = ['id','name','salary']
df= spark.createDataFrame(data=data,schema=schema)
df.show()

# COMMAND ----------

# DBTITLE 1,alias() Function
df1 = df.select(df.id.alias('emp_id'),df.name.alias('emp_name'),df.salary.alias('emp_salary'))
df1.show()

# COMMAND ----------

# DBTITLE 1,Desc() Function
df.sort(df.salary.desc()).show()

# COMMAND ----------

# DBTITLE 1,asc() Function
df.sort(df.salary.asc()).show()

# COMMAND ----------

# DBTITLE 1,Cast Function
df1 = df.select(df.id.cast('Integer'),df.name,df.salary.cast('Integer'))
df1.printSchema()

# COMMAND ----------

# DBTITLE 1,Like () operator
df.filter(df.name.like('%a_')).show()

# COMMAND ----------


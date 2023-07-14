# Databricks notebook source
data1 = [(1,'Arnab','male','IT'),(2,'Wafa','male','IT'),(3,'Maha','male','HR'),(4,'Annu','female','HR'),(5,'Shakti','female','IT'),(6,'Pradeep','male','HR'),(7,'Sarfaraz','male','HR'),(8,'Ayeesha','female','IT')]
schema1 = ['empid','name','gender','dept']

df= spark.createDataFrame(data=data1,schema=schema1)
df.show()

# COMMAND ----------

df.groupBy('dept','gender').count().show()

# COMMAND ----------

# DBTITLE 1,Using Pivot Function
df.groupBy('dept').pivot('gender').count().show()

# COMMAND ----------

# DBTITLE 1,Selecting a particular value of pivot column
df.groupBy('dept').pivot('gender',['male']).count().show()

# COMMAND ----------

df.groupBy('dept').pivot('gender',['male','female']).count().show()

# COMMAND ----------


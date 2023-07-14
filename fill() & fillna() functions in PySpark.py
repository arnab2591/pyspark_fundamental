# Databricks notebook source
data1 = [(1,'Arnab','male','IT'),(2,'Wafa',None,'IT'),(3,'Maha','male','HR'),(4,'Annu','female','HR'),(5,'Shakti','female','IT'),(6,'Pradeep','male','HR'),(7,'Sarfaraz','male','HR'),(8,'Ayeesha','female',None)]
schema1 = ['empid','name','gender','dept']

df= spark.createDataFrame(data=data1,schema=schema1)
df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

help(df.fillna)

# COMMAND ----------

df.fillna('unknown').show()

# COMMAND ----------

df.fillna('unknown',['gender','dept']).show()

# COMMAND ----------

df.na.fill('unknown',['dept']).show()

# COMMAND ----------

inputData = [(1,'Alex',5000,None),(2,'Liam',None,'uk'),(None,'Duckett',7000,'Eng'),(4,'Hope',7800,None)]

inputSchema=["Id","Name","Sal","Loc"]

inputDF = spark.createDataFrame(inputData,inputSchema)
inputDF.show()

# COMMAND ----------

inputDF.selectExpr( "*","coalesce(id,-1) as newID","coalesce(name,'unknown') as newName","coalesce(Sal,-1) as newSal","coalesce(Loc,'unknownLoc') as newLoc").show()

# COMMAND ----------


# Databricks notebook source
data1 = [(1,'Arnab','male','IT'),(2,'Wafa','male','IT'),(3,'Maha','male','HR'),(4,'Annu','female','HR'),(5,'Shakti','female','IT'),(6,'Pradeep','male','HR'),(7,'Sarfaraz','male','HR'),(8,'Ayeesha','female','IT')]
schema1 = ['empid','name','gender','dept']

df= spark.createDataFrame(data=data1,schema=schema1)
df.show()

# COMMAND ----------

df1 = df.select('empid')
df1.show()

# COMMAND ----------

help(df.collect)

# COMMAND ----------

listRows = df.collect()
print(listRows)

# COMMAND ----------

print(listRows[0]['name'])

# COMMAND ----------

print(listRows[0][1])

# COMMAND ----------

data = [(1,'Arnab',2000),(2,'wafa',3000)]
schema = ['id','name','salary']

df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

from pyspark.sql.functions import upper

def convertToUpper(df):
    return df.withColumn('name1', upper(df.name))

def doubleTheSalary(df):
    return df.withColumn('salary1',df.salary*2)

df1 = df.transform(convertToUpper)\
    .transform(doubleTheSalary)

df1.show()
    

# COMMAND ----------


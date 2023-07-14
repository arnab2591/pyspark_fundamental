# Databricks notebook source
data = [(1,'Arnab',2000),(2,'Wafa',4000)]

print(type(data))

# COMMAND ----------

rdd = spark.sparkContext.parallelize(data)
print(type(rdd))

# COMMAND ----------

print(rdd.collect())

# COMMAND ----------

df = rdd.toDF(schema=['id','name','salary'])
df.show()

# COMMAND ----------

df1= spark.createDataFrame(rdd,schema=['id','name','salary'])
df1.show()

# COMMAND ----------


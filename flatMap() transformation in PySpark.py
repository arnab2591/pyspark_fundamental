# Databricks notebook source
data = [ 'maheer shaik', 'wafa shaik','Arnab Chakraborty']

# COMMAND ----------

rdd = spark.sparkContext.parallelize(data)
print(rdd.collect())

# COMMAND ----------

for item in rdd.collect():
    print(item)

# COMMAND ----------

rdd1 = rdd.map(lambda x : x.split(" "))

for i in rdd1.collect():
    print(i)


# COMMAND ----------

rdd2 = rdd.flatMap(lambda x : x.split(" "))

for i in rdd2.collect():
    print(i)


# COMMAND ----------


# Databricks notebook source
# DBTITLE 1,RDD to DataFrame convert
data = [('maheer','shaik'),('wafa','shaik'),('Arnab','Chakraborty')]

rdd = spark.sparkContext.parallelize(data)

print(rdd.collect())

# COMMAND ----------

print(rdd.collect())

# COMMAND ----------

rdd1 = rdd.map(lambda x : x + (x[0]+" "+x[1],))

print(rdd1.collect())

# COMMAND ----------

df = spark.createDataFrame(rdd1,schema=['Fname','lname','Fullname'])

df.show()


# COMMAND ----------

# DBTITLE 1,DataFrame to RDD convert
data = [('maheer','shaik'),('wafa','shaik'),('Arnab','Chakraborty')]

df1 = spark.createDataFrame(data,schema=['Fname','lname'])
df1.show()

# COMMAND ----------

rdd2 = df1.rdd.map(lambda x: x+(x[0]+" "+x[1],))

print(rdd2.collect())

# COMMAND ----------

df2 = rdd2.toDF(['fn','ln','Fullname'])
df2.show()

# COMMAND ----------

def fullname(x):
    return x+(x[0]+" "+x[1],) 
 
rdd3 = df1.rdd.map(lambda x : fullname(x))

print(rdd3.collect())

df3 = rdd3.toDF(['fn','ln','Fullname']) 
df3.show()

# COMMAND ----------


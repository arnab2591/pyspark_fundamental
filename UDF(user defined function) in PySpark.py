# Databricks notebook source
data = [(1,'Arnab',2000,1000),(2,'Wafa',4000,500)]
schema = ['id','name','salary','bonus']
df= spark.createDataFrame(data=data,schema=schema)
display(df)

# COMMAND ----------

def totalPay(s,b):
    return s+b

from pyspark.sql.functions import udf
help(udf)



# COMMAND ----------

from pyspark.sql.types import IntegerType

TotalPayment = udf(lambda s,b:totalPay(s,b),IntegerType())

# COMMAND ----------

df.withColumn('totalpay',TotalPayment(df.salary,df.bonus)).show()

# COMMAND ----------

@udf(returnType=IntegerType())
def totalPay(s,b):
    return s+b

df.withColumn('totalpayment',totalPay(df.salary,df.bonus)).show()

# COMMAND ----------

df.select('*',totalPay(df.salary,df.bonus).alias('topay')).show()

# COMMAND ----------

# DBTITLE 1,UDF using pyspark sql
df.createOrReplaceTempView('emps')

def totalPay(s,b):
    return s+b

help(spark.udf.register)

# COMMAND ----------

spark.udf.register(name='TotalPaySQl',f=totalPay,returnType=IntegerType())

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT *,TotalPaySQL(salary,bonus) as totalpay from emps

# COMMAND ----------


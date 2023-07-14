# Databricks notebook source
Data = [(1,'maheer',['azure','dotnt']),(2,'wafa',['aws','java'])]

schema = ['id','name','skills']

df = spark.createDataFrame(Data,schema)
df.show()
df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import transform,upper

df.select('id','name',transform('skills',lambda x: upper(x)).alias('skills')).show()



# COMMAND ----------

def convertToUpper1(x):
    return upper(x)

from pyspark.sql.functions import transform
df.select(transform('skills',convertToUpper1).alias('skills')).show()

# COMMAND ----------


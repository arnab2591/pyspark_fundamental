# Databricks notebook source
data = [(1,'Arnab',4000),(2,'Wafa',3000)]
schema = ['id','name','salary']
df= spark.createDataFrame(data=data,schema=schema)
display(df)
df.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType

data = [(1,'Arnab',4000),(2,'Wafa',3000)]

schema=StructType([StructField(name='id',dataType=IntegerType()),\
                  StructField(name='name',dataType=StringType()),\
                  StructField(name='salary',dataType=IntegerType())])

df1= spark.createDataFrame(data=data,schema=schema)
df1.show()
df1.printSchema()

# COMMAND ----------

# DBTITLE 1,Nested StructType in StructFields for multiple columns
from pyspark.sql.types import StructType,StructField,StringType,IntegerType

data = [(1,('Arnab','Chakraborty'),4000),(2,('Wafa','Maheer'),3000)]

StructName = StructType([StructField(name='firstname',dataType=StringType()),\
            StructField(name='lastname',dataType=StringType())])

schema=StructType([StructField(name='id',dataType=IntegerType()),\
                  StructField(name='name',dataType=StructName),\
                  StructField(name='salary',dataType=IntegerType())])

df1= spark.createDataFrame(data=data,schema=schema)
df1.show()
df1.printSchema()

# COMMAND ----------

display(df1)

# COMMAND ----------


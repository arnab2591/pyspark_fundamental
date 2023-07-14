# Databricks notebook source
from pyspark.sql.functions import lit
col1 = lit("abcd")
print(type(col1))

# COMMAND ----------

data = [(1,'Arnab',2000),(2,'Wafa',4000)]
schema = ['id','name','salary']
df= spark.createDataFrame(data=data,schema=schema)
display(df)

# COMMAND ----------

df1 = df.withColumn('newCol',lit('newColval'))
df1.show()

# COMMAND ----------

df.select(df.name).show()

# COMMAND ----------

df.select(df['name']).show()

# COMMAND ----------

from pyspark.sql.functions import col
df.select(col('name')).show()

# COMMAND ----------

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

df1.select(df1.name.firstname).show()

# COMMAND ----------

df1.select(df1['name.firstname']).show()

# COMMAND ----------

df1.select(col('name.firstname')).show()

# COMMAND ----------


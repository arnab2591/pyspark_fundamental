# Databricks notebook source
# DBTITLE 1,Exploring explode Function
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,ArrayType
from pyspark.sql.functions import col,explode

data = [(1,'Arnab',['dotnet','azure']),(2,'wafa',['java','aws']),(3,'maheer',['rdbms','database'])]
schema = ['id','name','skills']
#schema = StructType([StructField('id',StringType()),\
 #                    StructField('numbers',ArrayType(IntegerType()))])

df = spark.createDataFrame(data=data,schema=schema)
display(df)
df.printSchema()

# COMMAND ----------

df.show()
df1 =df.withColumn('skill',explode(col('skills')))
df1.show()

# COMMAND ----------

# DBTITLE 1,Exploring split Function
from pyspark.sql.functions import split

data = [(1,'Arnab','dotnet,azure'),(2,'wafa','java,aws'),(3,'maheer','rdbms,database')]
schema = ['id','name','skills']
df = spark.createDataFrame(data=data,schema=schema)
display(df)
df.printSchema()

# COMMAND ----------


df.show()
df1 = df.withColumn('skill1',split(col('skills'),',')[0]).withColumn('skill2',split(col('skills'),',')[1]).withColumn('All_skill',split(col('skills'),','))
df1.show()

# COMMAND ----------

# DBTITLE 1,Exploring Array Function
from pyspark.sql.functions import array

data = [(1,'Arnab','dotnet','azure'),(2,'wafa','java','aws'),(3,'maheer','rdbms','database')]
schema = ['id','name','primaryskill','secondaryskill']
df = spark.createDataFrame(data=data,schema=schema)
display(df)
df.printSchema()


# COMMAND ----------

df.show()
df1 = df.withColumn('skills',array(col('primaryskill'),col('secondaryskill')))
df1.show()

# COMMAND ----------

# DBTITLE 1,Exploring Array_Contains Function
from pyspark.sql.functions import array_contains

data = [(1,'Arnab',['dotnet','azure']),(2,'wafa',['java','aws']),(3,'maheer',['rdbms','database'])]
schema = ['id','name','skills']

df = spark.createDataFrame(data=data,schema=schema)
display(df)
df.printSchema()

# COMMAND ----------

df.show()
df1 = df.withColumn('hasJavaskill',array_contains(col('skills'),'java'))
df1.show()

# COMMAND ----------


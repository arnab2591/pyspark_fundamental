# Databricks notebook source
data = [('abc',[1,2]),('mno',[4,5]),('xyz',[7,8])]
schema = ['id','numbers']

df = spark.createDataFrame(data=data,schema=schema)
df.show()
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Using ArrayType with StructType for defining schema of Dataframe
from pyspark.sql.types import StructType,StructField,StringType,IntegerType,ArrayType

data = [('abc',[1,2]),('mno',[4,5]),('xyz',[7,8])]
schema = StructType([StructField('id',StringType()),\
                     StructField('numbers',ArrayType(IntegerType()))])

df1 = spark.createDataFrame(data=data,schema=schema)
df1.show()
df1.printSchema()

# COMMAND ----------

help(ArrayType)

# COMMAND ----------

# DBTITLE 1,Fetching Data from another array Column
from pyspark.sql.functions import col
df2 = df1.withColumn(colName='firstNumber',col=col('numbers')[0])
display(df2)

# COMMAND ----------

df3 = df.withColumn(colName='firstNumber',col=col('numbers')[0])
display(df3)

# COMMAND ----------

df.withColumn('firstNumber',df.numbers[0]).show()

# COMMAND ----------

data = [(1,2),(3,4)]
schema = ['num1','num2']

df4 = spark.createDataFrame(data=data,schema=schema)
df4.show()

# COMMAND ----------

# DBTITLE 1,Putting two integer column data into one Array in a new column
from pyspark.sql.functions import array
df5 =df4.withColumn('numbers',array(col('num1'),col('num2')))
df5.show()

# COMMAND ----------

df5.printSchema()

# COMMAND ----------


# Databricks notebook source
from pyspark.sql.functions import col,lit

# COMMAND ----------

data = [(1,'Arnab','4000'),(2,'Wafa','3000')]
columns = ['id','name','salary']
df= spark.createDataFrame(data=data,schema=columns)
display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

help(df.withColumn)

# COMMAND ----------

# DBTITLE 1,Changing the column data type
df1 = df.withColumn(colName='salary',col=col('salary').cast('Integer'))
display(df1)
df1.printSchema()

# COMMAND ----------

# DBTITLE 1,Updating same column values
df2 = df1.withColumn(colName='salary',col=col('salary')*2)
df2.show()

# COMMAND ----------

# DBTITLE 1,Create a new column with new static value
df3 = df2.withColumn(colName='Country',col=lit('India'))
df3.show()

# COMMAND ----------

df4 = df3.withColumn(colName='copiedSalary',col=col('salary'))
df4.show()

# COMMAND ----------


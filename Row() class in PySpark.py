# Databricks notebook source
from pyspark.sql import Row
help(Row)

# COMMAND ----------

row = Row('maheer',2000)
print(row[0] + ' ' +str(row[1]))

# COMMAND ----------

row = Row(name ='maheer',Salary=2000)
print(row.name + ' ' +str(row.Salary))

# COMMAND ----------

row1 = Row(name ='maheer',Salary=2000)

row2 = Row(name ='Arnab',Salary=4000)
data = [row1,row2]
df = spark.createDataFrame(data)
df.show()
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Creating a Person as a Row class
Person = Row('name','Salary')

p1 = Person('maheer',2000)
p2 = Person('Arnab',4000)

data = [p1,p2]

df = spark.createDataFrame(data)
df.show()
df.printSchema()

# COMMAND ----------

from pyspark.sql import Row

data = [Row(name="Arnab",prop=Row(hair="black",eye="brown")),\
        Row(name="Maheer",prop=Row(hair="grey",eye="black"))]

df = spark.createDataFrame(data)
df.show()
df.printSchema()

# COMMAND ----------


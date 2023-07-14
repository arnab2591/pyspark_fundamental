# Databricks notebook source
from pyspark.sql.functions import row_number, rank,dense_rank

from pyspark.sql.window import Window

data = [('maheer','HR',2000),('wafa','IT',3000),('asi','HR',1500),('annu','payroll','3500'),('shakti','IT','3000'),
        ('pradeep','IT',4000),('Karnthi','payroll',2000),('Himanshu','IT',2000),('Bhargava','HR','2000'),('Arnab','IT',6000),('martin','IT',2000)]

schema = ['name','dep','salary']

df = spark.createDataFrame(data,schema)
df.show()

# COMMAND ----------

df.sort('dep').show()

# COMMAND ----------

window1 = Window.partitionBy('dep').orderBy('salary')

df.withColumn('rownumber',row_number().over(window1)).withColumn('rank',rank().over(window1)).withColumn('denserank',dense_rank().over(window1)).show()

# COMMAND ----------



# COMMAND ----------


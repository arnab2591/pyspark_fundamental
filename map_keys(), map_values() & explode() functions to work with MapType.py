# Databricks notebook source
data = [('Arnab',{'hair':'black','eye':'brown'}),('Maheer',{'hair':'black','eye':'blue'})]

schema = ['name','properties']
df = spark.createDataFrame(data,schema)
df.show(truncate=False)
display(df)
df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import explode

df1 = df.select('name','properties',explode(df.properties))
df1.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import map_keys,map_values

df2=df.withColumn('keys',map_keys(df.properties))
df2.show(truncate=False)

# COMMAND ----------

df3=df.withColumn('keys',map_values(df.properties))
df3.show(truncate=False)

# COMMAND ----------

df3=df.withColumn('keys',map_values(df.properties)).withColumn('seperate_values',explode('keys')).select('name','seperate_values')
df3.show(truncate=False)

# COMMAND ----------


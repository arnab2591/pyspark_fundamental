# Databricks notebook source
# DBTITLE 1,Defining Map Data Type
#Pyspark MapType is used to represent map key-value pair similar to python Dictionary(Dict)

data = [('Arnab',{'hair':'black','eye':'brown'}),('Maheer',{'hair':'black','eye':'blue'})]

schema = ['name','properties']
df = spark.createDataFrame(data,schema)
df.show(truncate=False)
display(df)
df.printSchema()

# COMMAND ----------

help(MapType)

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,MapType

data = [('Arnab',{'hair':'black','eye':'brown'}),('Maheer',{'hair':'black','eye':'blue'})]

schema =StructType([\
    StructField('name',StringType()),\
    StructField('properties',MapType(StringType(),StringType()))])

df = spark.createDataFrame(data,schema)
df.show(truncate=False)
display(df)
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Access of Map datatype
df1= df.withColumn('hair',df.properties['hair'])

df1.show(truncate=False)

# COMMAND ----------

display(df1.withColumn('eye',df1.properties.getItem('eye')))

# COMMAND ----------


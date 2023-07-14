# Databricks notebook source
data = [('maheer','{"hair":"black","eye":"brown"}')]

schema = ['name','props']

df = spark.createDataFrame(data, schema)
df.show()
df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import from_json

help(from_json)

# COMMAND ----------

from pyspark.sql.types import MapType,StringType

MapTypeschema = MapType(StringType(),StringType())

df1 = df.withColumn('propsMap',from_json(df.props,MapTypeschema))
df1.show(truncate=False)

# COMMAND ----------

df1.printSchema()

# COMMAND ----------

df2 = df1.withColumn('hair',df1.propsMap.hair)\
    .withColumn('eye',df1.propsMap.eye)

df2.show(truncate=False)

# COMMAND ----------

df.show(truncate=False)

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType

StructName = StructType([StructField(name='hair',dataType=StringType()),\
            StructField(name='eye',dataType=StringType())])

df3 = df.withColumn('propsstruct',from_json(df.props,StructName))
df3.show(truncate=False)
df3.printSchema()

# COMMAND ----------

df4 = df3.withColumn('hair',df3.propsstruct.hair)\
    .withColumn('eye',df3.propsstruct.eye)
df4.show()

# COMMAND ----------


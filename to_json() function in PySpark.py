# Databricks notebook source
from pyspark.sql.types import StructType,StructField,StringType
from pyspark.sql.functions import to_json

# COMMAND ----------

data = [('maheer',{"hair":"black","eye":"brown"})]

schema = ['name','props']

df = spark.createDataFrame(data, schema)
df.show()
df.printSchema()

# COMMAND ----------

help(to_json)

# COMMAND ----------

df1 = df.withColumn("prosString",to_json(df.props))
df1.show(truncate=False)
df1.printSchema()

# COMMAND ----------

data = [('maheer',('black','brown'))]

TupleSchema = StructType([StructField(name='hair',dataType=StringType()),\
                          StructField(name='eye',dataType=StringType())])


CompleteSchema = StructType([StructField(name='name',dataType=StringType())\
                            ,StructField(name='prosStruct',dataType=TupleSchema)])

df2 = spark.createDataFrame(data,CompleteSchema)

df2.show()

df2.printSchema()

# COMMAND ----------

df3=df2.withColumn('prosString',to_json(df2.prosStruct))
df3.show(truncate=False)
df3.printSchema()

# COMMAND ----------


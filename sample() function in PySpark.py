# Databricks notebook source
df = spark.range(start=1,end=101)
df.show()

# COMMAND ----------

# DBTITLE 1,Using Sample function to choose sample from dataframe
df1 = df.sample(fraction=0.1,seed=123)
df1.show()

# COMMAND ----------

df2 = df.sample(fraction=0.1,seed=123)
df2.show()

# COMMAND ----------


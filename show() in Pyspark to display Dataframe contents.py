# Databricks notebook source
data = [(1,'ghskdfdskhldf fmdsjbdfsllvmsdjdj'),
        (2,'Wafahxjidbhdfthsdjskdwdywdjsdydhsbjsbdyye'),
        (3,'qoeurfjfnjdfrrbvjrbrgbrbjdbh'),
        (4,'gfvfvfvwefvwvywtefywydfgtfd')]
schema = ['id','comments']
df= spark.createDataFrame(data=data,schema=schema)
display(df)

# COMMAND ----------

data = [(1,'ghskdfdskhldf fmdsjbdfsllvmsdjdj'),
        (2,'Wafahxjidbhdfthsdjskdwdywdjsdydhsbjsbdyye'),
        (3,'qoeurfjfnjdfrrbvjrbrgbrbjdbh'),
        (4,'gfvfvfvwefvwvywtefywydfgtfd')]
schema = ['id','comments']
df= spark.createDataFrame(data=data,schema=schema)
df.show()

# COMMAND ----------

help(df.show)

# COMMAND ----------

df.show(truncate=False)

# COMMAND ----------

df.show(truncate=10)

# COMMAND ----------

df.show(truncate=False,n=2)

# COMMAND ----------

df.show(truncate=False,n=4,vertical=True)

# COMMAND ----------


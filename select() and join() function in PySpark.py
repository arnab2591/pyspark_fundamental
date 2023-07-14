# Databricks notebook source
data = [(1,'Arnab',2000),(2,'Wafa',4000)]
schema = ['id','name','salary']
df= spark.createDataFrame(data=data,schema=schema)
display(df)

# COMMAND ----------

help(df.select)

# COMMAND ----------

df.select('name').show()

# COMMAND ----------

df.select(df.name).show()

# COMMAND ----------

df.select('name','salary').show()

# COMMAND ----------

df.select(['name','salary']).show()

# COMMAND ----------

from pyspark.sql.functions import col
df.select(col('id'),col('name')).show()

# COMMAND ----------

df.select('*').show()

# COMMAND ----------

df.select([col for col in df.columns]).show()

# COMMAND ----------

# DBTITLE 1,JOIN() Function
data1 = [(1,'Arnab',2000,2),(2,'Wafa',4000,1),(3,'Maha',6000,4)]
schema1 = ['empid','name','salary','deptid']

data2 = [(1,'IT'),(2,'HR'),(3,'Payroll')]
schema2 = ['id','dep']

df1= spark.createDataFrame(data=data1,schema=schema1)
df2= spark.createDataFrame(data=data2,schema=schema2)

df1.show()
df2.show()

# COMMAND ----------

help(df1.join)

# COMMAND ----------

df1.join(df2,df2.id==df1.deptid,'inner').show()

# COMMAND ----------

df1.join(df2,df2.id==df1.deptid,'left').show()

# COMMAND ----------

df1.join(df2,df2.id==df1.deptid,'right').show()

# COMMAND ----------

df1.join(df2,df2.id==df1.deptid,'full').show()

# COMMAND ----------

df1.join(df2,df2.id==df1.deptid,'leftsemi').show()

# COMMAND ----------

df1.join(df2,df2.id==df1.deptid,'leftanti').show()

# COMMAND ----------

# DBTITLE 1,self join condition
data1 = [(1,'Arnab',0),(2,'Wafa',1),(3,'Maha',2)]
schema1 = ['empid','name','managerid']

df3= spark.createDataFrame(data=data1,schema=schema1)
df3.show()

# COMMAND ----------

from pyspark.sql.functions import col

df3.alias('empData').join(df3.alias('mgrData'),\
    col('empData.empid')== col('mgrData.managerid'),\
        'inner').show()

# COMMAND ----------

df3.alias('empData').join(df3.alias('mgrData'),\
    col('empData.empid')== col('mgrData.managerid'),\
        'left').show()

# COMMAND ----------

df3.alias('empData').join(df3.alias('mgrData'),\
    col('empData.empid')== col('mgrData.managerid'),\
        'left').select(col('empData.name').alias('Emp_Name'),col('mgrData.name').alias('Manager_name')).show()

# COMMAND ----------


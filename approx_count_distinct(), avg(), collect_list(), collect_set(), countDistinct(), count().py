# Databricks notebook source
data = [(1,'Arnab','IT',2000),(2,'Wafa','HR',4000),(3,'Maheer','HR',2000)]
schema = ['empid','emp_name','dept','salary']
df= spark.createDataFrame(data=data,schema=schema)
df.show()

# COMMAND ----------

from pyspark.sql.functions import approx_count_distinct,avg,collect_list,collect_set,countDistinct,count

# COMMAND ----------

# We have two distnct value like 1500 and 3000
df.select(approx_count_distinct('salary')).show()
# calculating average of employee salary
df.select(avg('salary')).show()
#it will give list of all salry value
df.select(collect_list('salary')).show()
# It will give you distinct salary value in list
df.select(collect_set('salary')).show()
# It will give you the distinct count of salary value
df.select(countDistinct('salary')).show()
#It will give you the count of salary value
df.select(count('salary')).show()

# COMMAND ----------

df
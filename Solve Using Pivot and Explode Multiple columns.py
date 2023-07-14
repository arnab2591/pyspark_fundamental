# Databricks notebook source
# DBTITLE 1,Creating Data
l = [(1,'Gaga','India',"2022-01-11"),(1,'Katy','UK',"2022-01-11"),(1,'Bey','Europe',"2022-01-11"),(2,'Gaga',None,"2022-10-11"),(2,'Katy','India',"2022-10-11"),(2,'Bey','US',"2022-02-15"),(3,'Gaga','Europe',"2022-10-11"),(3,'Katy','US',"2022-10-11"),(3,'Bey',None,"2022-02-15"),(1,'Gaga','US',"2022-01-11"),(3,'Katy','Switz',"2022-02-15")]

# COMMAND ----------

# DBTITLE 1,Creating DataFrame
df = spark.createDataFrame(l,['ID','NAME','COUNTRY','Date_part'])
display(df)

# COMMAND ----------

# DBTITLE 1,Creating Aggregations with First function using Pivot
from pyspark.sql.functions import *
df1=df.groupBy('ID','Date_part').pivot('NAME').agg(first('Country'))
df1.show()

# COMMAND ----------

# DBTITLE 1,Creating Aggregations with Collect_list using Pivot
df1=df.groupBy('ID','Date_part').pivot('NAME').agg(collect_list('Country'))
display(df1)

# COMMAND ----------

# DBTITLE 1,Creating Array_zip column
df2 = df1.withColumn("new",arrays_zip("Bey","Gaga","Katy"))
display(df2)

# COMMAND ----------

# DBTITLE 1,Exploding the column
df2 = df1.withColumn("new",arrays_zip("Bey","Gaga","Katy")).withColumn("new1",explode("new"))
display(df2)

# COMMAND ----------

# DBTITLE 1,Dropping column
df2=df2.drop("new")
display(df2)

# COMMAND ----------

# DBTITLE 1,Selecting the exact columns which we need
df3 = df2.select("ID","DATE_part","new1.Bey","new1.Gaga","new1.Katy")
display(df3)

# COMMAND ----------

df1.columns

# COMMAND ----------

# DBTITLE 1,Picking columns from dataframe dynamically
list_column = df1.columns[2:]
for i in range(len(list_column)):
    list_column[i]="new1."+list_column[i]
    
list_column

# COMMAND ----------

# DBTITLE 1,Making the whole process dynamic
df2 = df1.withColumn("new",arrays_zip(*df1.columns[2:])).withColumn("new1",explode("new"))
df4 = df2.select(*df1.columns[0:2],*list_column)
display(df4)

# COMMAND ----------


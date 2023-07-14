# Databricks notebook source
df= spark.range(2)
df.show()

# COMMAND ----------

#datrType default yyyy-MM-dd
from pyspark.sql.functions import current_date

df1=df.withColumn('currentDate',current_date())
df1.show()

df1.printSchema()


# COMMAND ----------

# convert date format to different format
from pyspark.sql.functions import date_format
df2 = df1.withColumn('newFormat', date_format(df1.currentDate,'MM.dd.yyyy'))
df2.show()

# COMMAND ----------

# convert date string type to dateType
from pyspark.sql.functions import lit

df3=df2.withColumn("dateString",lit("2022-01-11"))
df3.show()

# COMMAND ----------

df3.printSchema()

# COMMAND ----------

from pyspark.sql.functions import to_date
df4=df3.withColumn("todate",to_date(df3.dateString,'dd-MM-yyyy'))
df4.show()
df4.printSchema()

# COMMAND ----------

# DBTITLE 1,datediff functions
from pyspark.sql.functions import datediff

df = spark.createDataFrame([('2015-06-14','2015-07-14')],['d1','d2'])
df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df1 = df.withColumn('datediff',datediff(df.d2,df.d1))
df1.show()

# COMMAND ----------

# DBTITLE 1,months_between() function
from pyspark.sql.functions import months_between
df2= df1.withColumn("monthdiff",months_between(df1.d2,df1.d1))
df2.show()

# COMMAND ----------

# DBTITLE 1,add_months
from pyspark.sql.functions import add_months
df.withColumn('addmonths',add_months(df.d2,3)).show()

# COMMAND ----------

from pyspark.sql.functions import add_months
df.withColumn('submonths',add_months(df.d2,-3)).show()

# COMMAND ----------

# DBTITLE 1,date_add
from pyspark.sql.functions import date_add
df.withColumn('adddays',date_add(df.d2,3)).show()
df.withColumn('subdays',date_add(df.d2,-3)).show()

# COMMAND ----------

# DBTITLE 1,year and month
from pyspark.sql.functions import year,month
df.withColumn('year',year(df.d2)).show()
df.withColumn('month',month(df.d2)).show()

# COMMAND ----------

df = spark.range(2)
df.show()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
df1=df.withColumn('currentTimeStamp',current_timestamp())
df1.show(truncate=False)
df1.printSchema()

# COMMAND ----------

df2=df1.withColumn('timestampInString',lit('12.25.2022 08.15.46'))
df2.show(truncate=False)
df2.printSchema()

# COMMAND ----------

help(to_timestamp)

# COMMAND ----------

from pyspark.sql.functions import to_timestamp,hour,minute,second
df3=df2.withColumn("timestampInString",to_timestamp(df2.timestampInString,'MM.dd.yyyy HH.mm.ss'))
df3.show(truncate=False)
df3.printSchema()

# COMMAND ----------

df1.select("*",hour(df1.currentTimeStamp),minute(df1.currentTimeStamp),second(df1.currentTimeStamp)).show(truncate=False)

# COMMAND ----------


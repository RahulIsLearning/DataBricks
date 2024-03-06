# Databricks notebook source
# DBTITLE 1,Import modules or libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *

# COMMAND ----------

# DBTITLE 1,Udf in python code
data = [
    (1,'rahul',2000,500),
    (2,'raj',4000,600),
    (3,'rakesh',6000,900),
    (4,'rosh',98000,1200),
    (5,'chetan',40000,1000)]

schema = ['id','name','salary','bonus']
df = spark.createDataFrame(data,schema)


# creating and registering function
@udf(returnType=IntegerType())
def totalPay(s,b):
    return s+b

#totalPayFunction = udf(lambda salary,bonus:totalPay(salary,bonus), IntegerType())

#df1 = df.withColumn('Total_Pay', totalPayFunction(df.salary, df.bonus))
#display(df1)

df2 = df.withColumn('Total_Pay', totalPay(df.salary, df.bonus))
display(df2)

# COMMAND ----------

# DBTITLE 1,Register function for spark sql
data = [
    (1,'rahul',2000,500),
    (2,'raj',4000,600),
    (3,'rakesh',6000,900),
    (4,'rosh',98000,1200),
    (5,'chetan',40000,1000)]

schema = ['id','name','salary','violations']
df = spark.createDataFrame(data,schema)
df.createOrReplaceTempView("emp")

# udf to use in sql query
def TotalPayIncludingViolations(s,v):
    return s-v

spark.udf.register(name = 'TotalPay', f=TotalPayIncludingViolations, returnType=IntegerType())

spark.sql("select *, TotalPay(salary,violations) as total_pay from emp").show()
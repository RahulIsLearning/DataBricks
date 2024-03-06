# Databricks notebook source
# DBTITLE 1,Import modules or libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *

# COMMAND ----------

data = [(1,'Rahul',5000),
        (2,'Rakesh',4000),
        (3,'Raj',3000),
        (4,'Rajshejkhar',8000),
        (5,'Ravan',9000)]

columns = ['id','name','salary']

#StructType is collection of StructField
schema = StructType([
    StructField(name= 'id', dataType=IntegerType()),
    StructField(name= 'name', dataType=StringType()),
    StructField(name= 'salary', dataType=IntegerType())
])

# COMMAND ----------

# DBTITLE 1,withColumn() and withColumnRenamed()
df = spark.createDataFrame(data,columns)
df1 = df.withColumnRenamed("salary","EmpSalary").withColumnRenamed("name","EmpName")
df2 = df1.withColumn("Bonus", when(col("EmpSalary")>4500,200).otherwise(300))
display(df2)

# COMMAND ----------

# DBTITLE 1,Create dataframe with simple schema
dfSchema = spark.createDataFrame(data, schema=schema)
display(dfSchema)

# COMMAND ----------

# DBTITLE 1,Complex Schema
data = [(1,('Rahul','Chanda'),5000),
        (2,('Rakesh','Patil'),4000),
        (3,('Raj','Kumar'),3000),
        (4,('Rajshejkhar','Bhoir'),8000),
        (5,('Ravan','Yadav'),9000)]

structName = StructType([StructField(name= 'first_name', dataType=StringType()),StructField(name= 'last_name', dataType=StringType())])

complexSchema = StructType([
    StructField(name= 'id', dataType=IntegerType()),
    StructField(name= 'name', dataType=structName),
    StructField(name= 'salary', dataType=IntegerType())
])

dfComplexSchema = spark.createDataFrame(data,complexSchema)
display(dfComplexSchema)
dfComplexSchema.printSchema()

# COMMAND ----------

# DBTITLE 1,ArrayType schema on Spark Dataframe
arrayData = [('abc',[1,2]),('mno',[3,4]),('juh',[6,7])]

arraySchema = StructType([
    StructField(name= 'name', dataType=StringType()),
    StructField(name= 'element', dataType=ArrayType(IntegerType()))
])

arrayDf = spark.createDataFrame(arrayData,arraySchema)
#display(arrayDf)
#arrayDf.printSchema()

arrayDf1 = arrayDf.withColumn("first_number", col("element")[0])  # access the array element with indexes
arrayDf2 = arrayDf1.withColumn("second_number", col("element")[1])  # access the array element with indexes
display(arrayDf2)

# COMMAND ----------

# DBTITLE 1,MapType schema on Spark Dataframe
#MapType is used to represent key-value pair similar to Python Dict

mapData = [
    ('Rahul',{'place':'chinchwad','city':'pune','state':'maharashtra'}),
    ('Pavan',{'place':'Bhalki','city':'Bidar','state':'Karnataka'}),
    ('Chetan',{'place':'Kalaburagi','city':'Kalaburagi','state':'Karnataka'})]

#mapSchema = ['name','address']

mapSchema = StructType([
    StructField('name', StringType()),
    StructField('address', MapType(StringType(),StringType()))
])

mapDf = spark.createDataFrame(mapData, mapSchema)
mapDf1 = mapDf.withColumn('city',mapDf.address['city'])   # access the value from map type

display(mapDf1)


# COMMAND ----------

# DBTITLE 1,Row() class
# represented as record/row in dataframe
# can create Row like class

# row1 = Row(name = 'Rahul',salary = 4000)
# row2 = Row(name = 'Raj',salary = 5000)
# row3 = Row(name = 'Rakesh',salary = 9000)
# print(row[0] +' - ' +str(row[1]))
# print(row.name +' - ' +str(row.salary))

# df = spark.createDataFrame([row1,row2,row3])
# display(df)

#------------------------------------------------------

# Person = Row("name","salary")
# p1 = Person('abc',44444)
# p2 = Person('rdf',634523)

# print(p1.name)
# print(p2.age)

#----------------- nested structure -------------------------

data = [Row(name = 'Rakesh', address = Row(city='Pune',state = 'MH')),
        Row(name = 'Pavan', address = Row(city='Solapur',state = 'MH')),
        Row(name = 'Chetan', address = Row(city='Bidar',state = 'KA')),
        Row(name = 'Roshini', address = Row(city='Banglore',state = 'KA'))]

df = spark.createDataFrame(data)
display(df)



# COMMAND ----------

# DBTITLE 1,Repartition vs Coalesce
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('SparkByExamples.com') \
        .master("local[5]").getOrCreate()

df=spark.range(0,20)
print(df.rdd.getNumPartitions())

df2 = df.repartition(6)
print(df2.rdd.getNumPartitions())

df3 = df.coalesce(2)
print(df3.rdd.getNumPartitions())

# COMMAND ----------


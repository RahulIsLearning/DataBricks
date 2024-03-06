# Databricks notebook source
# DBTITLE 1,Import libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import *

# COMMAND ----------

# DBTITLE 1,1. Calculate Expiry date based on Date
# calculate the Expiry date based on the Recharge_date and validity

data = [
    (1,20220511,1,'online'),
    (2,20220511,110,'online'),
    (3,20220511,35,'online'),
    (4,20220511,215,'online'),
    (5,20220511,43,'online')]

schema = StructType([
    StructField(name= 'Recharge_ID', dataType=IntegerType()),
    StructField(name= 'Recharge_Date', dataType=IntegerType()),
    StructField(name= 'Remaining_days', dataType=IntegerType()),
    StructField(name= 'status', dataType=StringType())
])

df = spark.createDataFrame(data,schema)

finaldf = df.withColumn("date_s",to_date(col("Recharge_Date").cast("string"),"yyyyMMdd")).withColumn("expiry_date", expr("date_add(date_s,Remaining_days)")).select("Recharge_ID","Recharge_Date","Remaining_days","expiry_date")

finaldf.show()

# COMMAND ----------

# DBTITLE 1,Adding a single column to complex schema
data = [
    (("James","","Smith"),"36636","M",3100),
    (("Michael","Rose",""),"40288","M",4300),
    (("Robert","","Williams"),"42114","M",1400),
    (("Maria","Anne","Jones"),"39192","F",5500),
    (("Jen","Mary","Brown"),"","F",-1)
  ]

schema = StructType([
        StructField('name', StructType([
             StructField('firstname', StringType(), True),
             StructField('middlename', StringType(), True),
             StructField('lastname', StringType(), True)
             ])),
         StructField('id', StringType(), True),
         StructField('gender', StringType(), True),
         StructField('salary', IntegerType(), True)])

df = spark.createDataFrame(data,schema)
df.printSchema()


# adding the simple schema into complex schema
updatedDF = df.withColumn("OtherInfo", struct(col("id").alias("id"),
                                              col("gender").alias("gender"),
                                              col("salary").alias("salary"),
                                              when(col("salary").cast(IntegerType()) < 2000,"Low")
                                              .when(col("salary").cast(IntegerType()) < 4000,"Medium")
                                              .otherwise("High").alias("salary_grade"))).drop("id","gender","salary")
updatedDF.printSchema()


#adding an new column into complex schema
s_field = df.schema["name"].dataType.names # to get all column names under "name" structtype field
updatedDf1 = df.withColumn("name", struct(*([col('name')[c].alias(c) for c in s_field] + [concat(col("name.firstname"),lit('|'),col("name.middlename"),lit('|'),col("name.lastname")).alias("FullName")])))
updatedDf1.printSchema()
updatedDf1.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Split a single column to multiple columns

data=data = [('James','','Smith','1991-04-01'),
  ('Michael','Rose','','2000-05-19'),
  ('Robert','','Williams','1978-09-05'),
  ('Maria','Anne','Jones','1967-12-01'),
  ('Jen','Mary','Brown','1980-02-17')
]

schema=["firstname","middlename","lastname","dob"]


df = spark.createDataFrame(data,schema)
df.printSchema()


df1 = df.withColumn('year', split(df['dob'], '-').getItem(0)) \
       .withColumn('month', split(df['dob'], '-').getItem(1)) \
       .withColumn('day', split(df['dob'], '-').getItem(2))
df1.show(truncate=False)


split_col = split(df['dob'], '-')
df3 = df.select("firstname","middlename","lastname","dob", split_col.getItem(0).alias('year'),split_col.getItem(1).alias('month'),split_col.getItem(2).alias('day'))   
df3.show(truncate=False)


# COMMAND ----------

# DBTITLE 1,Drop single column from complex schema
data = [
    (("James","","Smith"),"36636","M",3100),
    (("Michael","Rose",""),"40288","M",4300),
    (("Robert","","Williams"),"42114","M",1400),
    (("Maria","Anne","Jones"),"39192","F",5500),
    (("Jen","Mary","Brown"),"","F",-1)
  ]

schema = StructType([
        StructField('name', StructType([
             StructField('firstname', StringType(), True),
             StructField('middlename', StringType(), True),
             StructField('lastname', StringType(), True)
             ])),
         StructField('id', StringType(), True),
         StructField('gender', StringType(), True),
         StructField('salary', IntegerType(), True)])

df = spark.createDataFrame(data,schema)

df1 = df.withColumn("name", col("name").dropFields("middlename"))
df1.printSchema()
df1.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,cross join spark
#calculate the travel time
spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

data = [(1,"station1","4:20 AM"),(1,"station2","5:30 AM"),(1,"station3","7:20 AM"),
        (2,"station2","5:20 AM"),(2,"station4","7:30 AM"),(2,"station5","8:20 AM"),(2,"station6","12:20 AM")]

df = spark.createDataFrame(data,["id","station","time"])

#df1 = df.join(df,how='cross',on='id') # here we get duplicates for stations

#applying windwing function
windowspec = Window.partitionBy("id").orderBy(to_timestamp("time",'hh:mm a').asc())
df_wind = df.withColumn("row_num", row_number().over(windowspec))

final_df = df_wind.alias("a").join(df_wind.alias("b"), (col("a.row_num") < col("b.row_num")) & (col("a.id")==col("b.id"))).select(col("a.id").alias("id"), col("a.station").alias("source_point"), col("a.time").alias("source_time"), col("b.station").alias("destination_point"), col("b.time").alias("destination_time"))

display(final_df)

# COMMAND ----------

# DBTITLE 1,Check if column exists in dataframe
data = [
    (1,20220511,1,'online'),
    (2,20220511,110,'online'),
    (3,20220511,35,'online'),
    (4,20220511,215,'online'),
    (5,20220511,43,'online')]

schema = StructType([
    StructField(name= 'Recharge_ID', dataType=IntegerType()),
    StructField(name= 'Recharge_Date', dataType=IntegerType()),
    StructField(name= 'Remaining_days', dataType=IntegerType()),
    StructField(name= 'status', dataType=StringType())
])

df = spark.createDataFrame(data,schema)

fields = df.schema.fieldNames()
if fields.count('status') > 0:
    print('status col is present')
else:
    print('col not present')
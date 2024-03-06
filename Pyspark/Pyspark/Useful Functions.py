# Databricks notebook source
# DBTITLE 1,Import modules or libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Array Functions explode()
#explode() to create a new row for each element in given array column

arrExplodeData = [(1, 'Rahul', ['Spark','Python','Scala']),
    (2, 'Akash', ['C++','Python']),
    (3, 'Raj', ['DotNet','Angular'])]

arrExplodeSchema = ['id','name','skills']

arrExplodeDf = spark.createDataFrame(arrExplodeData,arrExplodeSchema)
display(arrExplodeDf)
arrExplodeDf1 = arrExplodeDf.withColumn('explodedColumn',explode(col('skills'))).select('id','name','explodedColumn')
display(arrExplodeDf1)

# COMMAND ----------

# DBTITLE 1,Array Functions split()
#split() returns an array type after splitting the string type by delimiter

#explode() to create a new row for each element in given array column

arrSplitData = [(1, 'Rahul', 'Spark|Python|Scala'),
    (2, 'Akash', 'C++|Python'),
    (3, 'Raj', 'DotNet|Angular')]

arrSplitSchema = ['id','name','skills']

arrSplitDf = spark.createDataFrame(arrSplitData,arrSplitSchema)
display(arrSplitDf)
arrSplitDf1 = arrSplitDf.withColumn('splitColumn',split(col('skills'),'\\|')).select('id','name','splitColumn')
display(arrSplitDf1)

# COMMAND ----------

# DBTITLE 1,Array Functions array_contains()
# array_contains() used to check if any array column contains a value

arrData = [
    (1, 'Rahul', ['Spark','Python','Scala']),
    (2, 'Akash', ['C++','Python']),
    (3, 'Raj', ['DotNet','Angular'])]

schema = ['id','name','skills']

dfData = spark.createDataFrame(arrData,schema)
display(dfData)
dfData1 = dfData.withColumn("hasPythonSkill", array_contains(col("skills"),"Python")).select('id','name','hasPythonSkill')
display(dfData1)

# COMMAND ----------

# DBTITLE 1,Combine Columns to form ArrayType
#Combine Columns to form ArrayType

arrData = [(1,2),(3,4)]
arrSchema = ['num1','num2']


arrDf = spark.createDataFrame(arrData,arrSchema)
display(arrDf)
arrDf1 = arrDf.withColumn('num_array',array(col("num1"),col("num2")))
display(arrDf1)

# COMMAND ----------

# DBTITLE 1,MapType Function explode()
mapData = [
    ('Rahul',{'place':'chinchwad','city':'pune','state':'maharashtra'}),
    ('Pavan',{'place':'Bhalki','city':'Bidar','state':'Karnataka'}),
    ('Chetan',{'place':'Kalaburagi','city':'Kalaburagi','state':'Karnataka'})]

#mapSchema = ['name','address']

mapSchema = StructType([
    StructField('name', StringType()),
    StructField('address', MapType(StringType(),StringType()))
])

explodeDf = spark.createDataFrame(mapData, mapSchema)
display(explodeDf)
explodeDf1 = explodeDf.select('name','address',explode(explodeDf.address))
display(explodeDf1)

# COMMAND ----------

# DBTITLE 1,MapType Function map_keys() and map_values()
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
display(mapDf)
mapDf1 = mapDf.withColumn('map_keys',map_keys(mapDf.address)).withColumn('map_values',map_values(mapDf.address))
display(mapDf1)

# COMMAND ----------

# DBTITLE 1,when and otherwise Functions 
# similar to case when statements

data = [('Rahul','Male'),('Roshini','Female'),('Rakesh','Male')]

schema = ['name','gender']

df = spark.createDataFrame(data,schema)
display(df)
df1 = df.withColumn("sex", when(col("gender") == 'Male', 'M').otherwise('F')).select('name','sex')
display(df1)

# COMMAND ----------

# DBTITLE 1,Column Functions
data = [(1,'rah',53463),(2,'rosh',1234),(3,'che',98796)]
schema = ['id','name','salary']

df = spark.createDataFrame(data,schema)
#display(df)

#alias function

df1 = df.select(col('id').alias('emp_id'), col('name').alias('emp_name'), col('salary').alias('emp_Salary'))
#display(df1)

# asc() and desc() function
df2 = df.sort(col('salary').desc())
#display(df2)

# cast() function
df3 = df.select('id','name',col('salary').cast('int'))
#df3.printSchema()

#like operator
df4 = df.filter(col('name').like('r%')).select('id','name','salary')
#display(df4)

# COMMAND ----------

# DBTITLE 1,distinct() and dropDuplicates()
data = [(1,'rah','male'),(2,'rosh','female'),(2,'rosh','female'),(3,'che','male')]
schema = ['id','name','gender']

df = spark.createDataFrame(data,schema)
display(df)
#display(df.distinct())
display(df.dropDuplicates())

# COMMAND ----------

# DBTITLE 1,orderBy() and sortBy() Functions
#sort() method will sort the records in each partition and then return the final output which means that the order of the output data is not guaranteed because the data is ordered on partition-level. sort() method is efficient thus more suitable when sorting is not critical for your use-case.

# the orderBy() function guarantees a total order in the output. This happens because the data will be collected into a single executor in order to be sorted.

data = [(1,'rah',53463),(2,'rosh',1234),(3,'che',98796),(4,'pav',78980)]
schema = ['id','name','salary']

df = spark.createDataFrame(data,schema)
df1 = df.sort(col("salary").desc())
display(df1)

# COMMAND ----------

# DBTITLE 1,GroupBy Functions
data = [(1,'RAH','M',50000,'Developer'),
       (2,'GHI','F',60000,'HR'),
       (3,'RGH','F',80000,'IT'),
       (4,'UIJ','M',50000,'Developer'),
       (5,'ERD','M',90000,'HR'),
       (6,'TRD','F',20000,'IT'),
       (7,'NBH','M',60000,'Developer'),
       (8,'DES','F',56000,'HR')]
schema = ['id','name','gender','salary','dep']

df = spark.createDataFrame(data,schema)
display(df)
#df1 = df.groupBy('dep').count()
#display(df1)

#---------------------------------------------------

df1 = df.groupBy('dep').agg(count(col('name')).alias('cntEmp'), 
                           min('salary').alias('minSalary'),
                           max('salary').alias('maxSalary'),
                           sum('salary').alias('totalSalaryDep'))
display(df1)

# COMMAND ----------

# DBTITLE 1,unionByName() Function
data1 = [(1,'rah',26)]
schema1 = ['id','name','age']

data2 = [(1,'rah',40000)]
schema2 = ['id','name','salary']

df1 = spark.createDataFrame(data1,schema1)
df2 = spark.createDataFrame(data2,schema2)

df = df1.union(df2)  # union based on index and column numbers
display(df)

df3 = df1.unionByName(df2, True) # union based on column name
display(df3)


# COMMAND ----------

# DBTITLE 1,Join Functions
# Inner, Left, Outer, Right Outer, Left Outer, Left Anti, Left Semi, Cross Self

empdata = [(1, 'rahul',4000,2), (2, 'raj',5000,1), (3, 'pav', 8000, 4)]
schema1 = ['id','name','salary','depid']

depdata = [(1,'IT'),(2,'HR'),(3,'Developer')]
schema2 = ['depid','depname']

empDf = spark.createDataFrame(empdata,schema1)
depDf = spark.createDataFrame(depdata,schema2)

resDf = empDf.join(depDf, empDf['depid']==depDf['depid'],"inner").select(empDf['name'],empDf['salary'],depDf['depname']) #'inner', 'outer', 'full', 'fullouter', 'full_outer', 'leftouter', 'left', 'left_outer', 'rightouter', 'right', 'right_outer', 'leftsemi', 'left_semi', 'semi', 'leftanti', 'left_anti', 'anti', 'cross'
display(resDf)

# COMMAND ----------

# DBTITLE 1,pivot() function
# used to rotate data in one column into multiple columns
# It is an aggregation where one of grouping columns will be converted into individual

data = [(1,'RAH','M',50000,'Developer'),
       (2,'GHI','F',60000,'HR'),
       (3,'RGH','F',80000,'IT'),
       (4,'UIJ','M',50000,'Developer'),
       (5,'ERD','M',90000,'HR'),
       (6,'TRD','F',20000,'IT'),
       (7,'NBH','M',60000,'Developer'),
       (8,'DES','F',56000,'HR')]
schema = ['id','name','gender','salary','dep']

df = spark.createDataFrame(data,schema)
display(df)
df1 = df.groupBy('dep').pivot('gender',['M','F']).count()
display(df1)

# COMMAND ----------

# DBTITLE 1,unpivot function - stack()
#rotating columns into rows
# we use stack() function

data = [('IT', 8, 5),('HR',7,2),('DEVELOPER',9,1)]
schema = ['dep','male','female']

df = spark.createDataFrame(data,schema)
display(df)
df1 = df.select('dep',expr("stack(2, 'M' ,male, 'F' ,female) as (gender,count)"))
display(df1)

# COMMAND ----------

# DBTITLE 1,fill() and fillna() functions
#fillna function is used to replace NULL/None values on all or selected columns


data = [
    (1,'rahul','male',1000),
    (2,'rakesh','male',None),
    (3,'roshini',None,6000)]

schema = ['id','name','gender','salary']

df = spark.createDataFrame(data,schema)
display(df)
df1 = df.fillna('UNKNOWN',['gender']).fillna(0,['salary'])
display(df1)

# COMMAND ----------

# DBTITLE 1,dataframe.transform() function
# it is used to chain the custom transformations and this function returns the new dataframe after applying the specified transformations

data = [(1,'rahul',5000,'pune'),(2,'jay',7000,'nagpur'),(3,'soham',6000,'nagar')]
schema = ['id','name','salary','city']
df = spark.createDataFrame(data,schema)

def capitalizeFirstletter(df):
    return df.withColumn('city', initcap(col("city")))

def convertToUpper(df):
    return df.withColumn('name', upper(df.name))

def doubleTheSalary(df):
    return df.withColumn('salary', df.salary*2)

df1 = df.transform(convertToUpper).transform(doubleTheSalary).transform(capitalizeFirstletter)
display(df)
display(df1)

# COMMAND ----------

# DBTITLE 1,pyspark.sql.functions.transform()
# it is used to apply the transformation on column type of Array

data = [
    (1,'rahul',['ra','hul']),
    (1,'rakesh',['ra','ke','sh']),
    (1,'roshini',['ro','shi','ni']),
    (1,'chetan',['ch','et','an'])]

schema = ['id','name','elements']

df = spark.createDataFrame(data,schema)

def convertToUpper(x):
    return upper(x)

df1 = df.select('id','name',transform('elements', lambda x: upper(x)).alias('elements'))
display(df1)
df2 = df.select(transform('elements', convertToUpper).alias('transform_elements'))
display(df2)

# COMMAND ----------

# DBTITLE 1,map() function
data = [('rahul','chanda'),('chetan','ghale'),('tushar','bammani')]

rdd = spark.sparkContext.parallelize(data)

rdd1 = rdd.map(lambda x: x + (x[0]+ ' '+x[1],))
print(rdd1.collect())


df = spark.createDataFrame(data,['fn','ln'])
df1 = df.rdd.map(lambda x : x + (x[0]+ ' '+x[1],)).toDF(['fn','ln','full_name'])
df1.show()

# COMMAND ----------

# DBTITLE 1,flatmap() function
data = [('rahul chanda'),('chetan ghale'),('tushar bammani')]

rdd = spark.sparkContext.parallelize(data)

for item in rdd.collect():
    print(item)
print("\n")   
rdd1 = rdd.flatMap(lambda x:x.split(' '))
for item in rdd1.collect():
    print(item)

# COMMAND ----------

# DBTITLE 1,partitionBy() function
data = [(1,'rahul','chanda','M',26),
        (2,'rakesh','chanda','M',22),
        (3,'roshini','chanda','F',18),
        (4,'diya','chanda','F',14),
        (5,'chetan','ghale','M',25)]

schema = ['id','fname','lname','gender','age']

df = spark.createDataFrame(data,schema)
df.write.partitionBy('gender').csv('/mnt/raw/temp/rahulc/Emp/',mode = 'overwrite')

# COMMAND ----------

# DBTITLE 1,from_json 
# convert json string into MapType

data = [('rahul',"{'hair':'black','eyes':'brown','gender':'male'}")]
schema = ['id','prop']

df = spark.createDataFrame(data,schema)
df.printSchema()
MapTypeSchema = MapType(StringType(),StringType())

df1 = df.withColumn('propMap',from_json(df.prop, MapTypeSchema))
df1.printSchema()

df_final = df1.withColumn('hair',df1.propMap.hair).withColumn('eye',df1.propMap.eyes).withColumn('gender',df1.propMap.gender)
display(df_final)

# COMMAND ----------

# DBTITLE 1,from_json function
# convert json string into StructType and StructField

data = [('rahul',"{'hair':'black','eyes':'brown','gender':'male'}")]
schema = ['id','prop']

df = spark.createDataFrame(data,schema)
df.printSchema()
StructTypeSchema = StructType([\
                              StructField('hair',StringType()),\
                              StructField('eyes',StringType()),\
                              StructField('gender',StringType())])

df1 = df.withColumn('propMap',from_json(df.prop, StructTypeSchema))
df1.printSchema()

df_final = df1.withColumn('hair',df1.propMap.hair).withColumn('eye',df1.propMap.eyes).withColumn('gender',df1.propMap.gender)
display(df_final)

# COMMAND ----------

# DBTITLE 1,to_json function
data = [('rahul',{'hair':'black','eyes':'brown','gender':'male'})]
schema = ['id','prop']
df = spark.createDataFrame(data,schema)
df.printSchema()

df1 = df.withColumn('jsonStringSchema',to_json(df.prop))
df1.printSchema()
display(df1)


# COMMAND ----------

# DBTITLE 1,json_tuple()
#used to extract some elements from json string

data = [('rahul',"{'hair':'black','eyes':'brown','gender':'male'}")]
schema = ['id','prop']
df = spark.createDataFrame(data,schema)
df.printSchema()

df1 = df.select(df.id, json_tuple(df.prop,'hair','eyes').alias('hair','eyes'))
display(df1)

# COMMAND ----------

# DBTITLE 1,get_json_object
# extract json string based on the path from json column

data = [('rahul',"{'hair':'black','eyes':'brown','gender':'male'}","{'address':{'city':'Pune','state':'maharashtra'}}")]
schema = ['id','prop','address']
df = spark.createDataFrame(data,schema)
df.printSchema()
display(df)
df1 = df.select(df.id, get_json_object('address','$.address.city').alias('city'))
display(df1)

# COMMAND ----------

# DBTITLE 1,Date Functions in Pyspark
df_date = spark.range(2)

df_date1 = df_date.withColumn('currentDate', current_date()) #yyyy-MM-dd is defualt format
df_date1.printSchema()
display(df_date1)

df_date2 = df_date1.withColumn('currentChangeDate', date_format(col('currentDate'), 'dd-MM-yyyy')) # change the format of date 
df_date2.printSchema()
display(df_date2)

# COMMAND ----------

# DBTITLE 1,Date Function in pyspark
df = spark.createDataFrame([('2023-03-14','2023-01-18')], ['D1','D2'])
display(df)

df1 = df.withColumn('date_difference', datediff(col('D1'), col('D2'))) \
        .withColumn('month_difference', months_between(col('D1'),col('D2'))) \
        .withColumn('addMonth', add_months(col('D1'), 4)) \
        .withColumn('substractMonth', add_months(col('D2'),-5)) \
        .withColumn('AddDays', date_add(col('D2'), 10)) \
        .withColumn('Year', year(col('D2'))) \
        .withColumn('Month', month(col('D2'))) \

display(df1)

# COMMAND ----------

# DBTITLE 1,Timestamp functions 
# defaukt value is yyyy-MM-dd HH:mm:ss:SS
df = spark.range(2)
df1 = df.withColumn('current timestamp',current_timestamp()) \
        .withColumn('timestampInString', lit('12.25.2023 08.09.09')) \
        .withColumn('timestampFromString',to_timestamp(col('timestampInString'), 'MM.dd.yyyy HH.mm.ss'))

display(df1)

# COMMAND ----------

# DBTITLE 1,Aggregate Functions
# operates on a group of rows and calculate a single return value for every group

# 1. approx_count_distinct() - returns count of distinct items in group
# 2. avg() - return avg of values in group
# 3. collect_list() - return all values from input as list with dups
# 4. collect_set() - return all values from input as set without dups
# 5. countDistinct() - retuns no. of distinct elements in col


data = [(1,'rahul','chanda','M',26),
        (2,'rakesh','chanda','M',22),
        (3,'roshini','chanda','F',18),
        (4,'diya','chanda','F',14),
        (5,'chetan','ghale','M',26)]

schema = ['id','fname','lname','gender','age']

df = spark.createDataFrame(data,schema)
display(df)
df.select(approx_count_distinct('age')).show()
df.select(avg('age')).show()
df.select(collect_list('age')).show()
df.select(collect_set('age')).show()
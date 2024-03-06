# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

# COMMAND ----------

# DBTITLE 1,Handling Null values 
data = [(1,"a",25), (2,"b",None), (3,None,30)]
schema = ["id","name","age"]

df = spark.createDataFrame(data,schema)

# dropping null values
df1 = df.dropna() 
df1.show()

# filling 0 for null values
df2 = df.fillna(0, subset = ["age"])
df2.show()

#filling avg for null values
df3 = df.withColumn("age", when(col("age").isNull(),df.select(avg("age")).first()[0]).otherwise(col("age")))
df3.show()

# COMMAND ----------

# DBTITLE 1,Situation 1
data = [
    (101,'Mark',"White Tiger"),
    (102,'Ria',"The FountainHead"),
    (102,'Ria',"Secret History"),
    (101,'Mark',"Bhagwad Geeta"),
    (103,'Loi',"Discover India")
]

schema = ["Student_id","Student_name","Book_issued"]

df = spark.createDataFrame(data,schema)
display(df)

df_sol1 = df.groupby("Student_id","Student_name").agg(concat_ws(",", collect_list(df["Book_issued"])).alias("Books"))
display(df_sol1)


# viceversa

df_rev = df_sol1.withColumn('exploded_Column', explode(split(df_sol1["Books"], ',')))
display(df_rev)

# COMMAND ----------

# DBTITLE 1,Situation 2
student = [(1,"shyam"), (2, "Ram"), (3,"Amit")]
marks = [(1,1,90),(1,2,100),(2,1,70),(2,2,60),(3,1,30),(3,2,20)]
s1 = ["id","student_name"]
s2 = ["student_id","subject","marks"]

df1 = spark.createDataFrame(student,s1)
df2 = spark.createDataFrame(marks,s2)

display(df1)
display(df2)

temp_res = df1.join(df2, df1["id"]==df2["student_id"])
grouped_df = temp_res.groupBy("id","student_name").agg(
    max(temp_res["marks"]).alias("max"))

res_df = grouped_df.withColumn("result", when(grouped_df["max"] >= 75,"distinction").when((grouped_df["max"] >=40) & (grouped_df["max"]<75),"second_class").otherwise("fail"))

display(res_df)

# COMMAND ----------

# DBTITLE 1,Situation 3: using unionByName
b1 = [("Delhi","Neha",90)]
b2 = [("Arav","Kolkata",79,83), (None,"Kolkata",39,13)]

s1 = ["Branch","Student","Maths_marks"]
s2 = ["Student","Branch","Science_marks","Maths_marks"]

d1 = spark.createDataFrame(b1,s1)
d2 = spark.createDataFrame(b2,s2)

display(d1)
display(d2)

resdf = d1.unionByName(d2, allowMissingColumns=True).fillna(0,subset=["Science_marks"]).fillna("Unknown",subset=["Student"])
display(resdf)

# COMMAND ----------

# DBTITLE 1,Situation 4
data =[
    (1,"John",30,"Sales",50000)
    ,(2,"Alice",28,"Marketing",60000)
    ,(3,"Bob",32,"Finance",90000)
    ,(4,"Sarah",39,"Sales",30000)]

schema = ["id","name","age","dept","salary"]

df = spark.createDataFrame(data,schema)
display(df)

# 1. Calculate the avg salary
avg_df = df.groupBy("dept").agg(avg(df["salary"]))
# display(avg_df)

# 2. Add new column "bonus" with 10% of salary
bonus_df = df.withColumn("bonus", 0.1 * df["salary"])
# display(bonus_df)

# 3. find employee with highest salary in each dept
w = Window.partitionBy("dept").orderBy(df["salary"].desc())
h = df.withColumn("row_num", row_number().over(w))
# display(h.filter(h["row_num"]==1).drop("row_num"))

# 4. Find top 3 dept with highest total salary
top3 = df.groupBy("dept").agg(sum("salary").alias("totalSalaryByDept")).orderBy(desc("totalSalaryByDept"))
# display(top3)

# 5. Top most dept having salary
gp1 = df.groupBy("dept").agg(sum("salary").alias("total_Sal"))
w = Window.orderBy(gp1["total_Sal"].desc())
dept_rn = gp1.withColumn("rn",row_number().over(w))
res = dept_rn.filter(dept_rn["rn"]==1).select("dept")
# display(res)

# 6. Calculate the diff between each emp salary and avg salary of each dept
w = Window.partitionBy("dept")
r1 = df.withColumn("avg_sal_by_dept", avg(col("salary")).over(w))
r2 = r1.withColumn("diff_sal_by_dept", col("salary")-col("avg_sal_by_dept"))

# COMMAND ----------

# DBTITLE 1,Find Origin and Destination of each customer
data = [
    (1,'Flight1','Delhi','Hyderabad'),
    (1,'Flight3','Pune','Bhopal'),
    (1,'Flight2','Hyderabad','Pune'),
    (2,'Flight1','Mumbai','Banglore'),
    (2,'Flight2','Banglore','Jaipur'),
]

schema = ["cust_id","flight_id","origin_str","dest_str"]

df = spark.createDataFrame(data,schema)
display(df)

#==============================================================

# Solution 1:
# ws = Window.partitionBy(df["cust_id"]).orderBy(df["flight_id"])
# df1 = df.withColumn("rn", row_number().over(ws))
# df2 = df1.groupBy(df1["cust_id"]).agg(min(df1['rn']).alias('origin'), max(df1['rn']).alias("destination"))
# res_Df = df1.join(df2, df1["cust_id"]==df2["cust_id"]).drop(df2['cust_id'])
# finalDf = res_Df.groupBy(res_Df['cust_id']).agg(max(when(res_Df["rn"] == res_Df["origin"], res_Df["origin_str"] )).alias("start"), 
#                                                                                  max(when(res_Df["rn"] == res_Df["destination"], res_Df["dest_str"])).alias("end"))
# display(finalDf)

#==============================================================

# Solution 2:
ws1 = Window.partitionBy(df['cust_id']).orderBy(df["flight_id"]).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
df_sln1 = df.withColumn("start", first(df['origin_str']).over(ws1)).withColumn("end", last(df['dest_str']).over(ws1))
finalDf1 = df_sln1.groupBy(df_sln1["cust_id"]).agg(max(df_sln1["start"]).alias("starting_point"), max(df_sln1["end"]).alias("ending_point"))
display(finalDf1)

# COMMAND ----------


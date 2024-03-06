# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

schema1 = StructType() \
      .add("SN",StringType(),True) \
      .add("BBMP_Zone",StringType(),True) \
      .add("Ward_Number",StringType(),True) \
      .add("Ward_Name",StringType(),True) \
      .add("Date_of_identification_as_Containment_zone",StringType(),True) \
      .add("Total_Number_of_positive_cases",StringType(),True) \
      .add("Containment_Zone_ID",StringType(),True) \
      .add("Date_of_return_to_normal",StringType(),True) \
      .add("Status",StringType(),True)


schema2 = (
    StructType()
    .add("index", StringType(), True)
    .add("Date", StringType(), True)
    .add("Year", StringType(), True)
    .add("Month", StringType(), True)
    .add("Customer_Age", StringType(), True)
    .add("Customer_Gender", StringType(), True)
    .add("Country", StringType(), True)
    .add("State", StringType(), True)
    .add("Product_Category", StringType(), True)
    .add("Sub_Category", StringType(), True)
    .add("Quantity", StringType(), True)
    .add("Unit_Cost", StringType(), True)
    .add("Unit_Price", StringType(), True)
    .add("Cost", StringType(), True)
    .add("Revenue", StringType(), True)
    .add("Column1", StringType(), True)
)



# COMMAND ----------

# DBTITLE 1,Read raw data
def read_rawdata(table_name,input_filename,input_Schema):    
    @dlt.table(
        name = table_name, 
        comment=f"load the table {table_name}"
    )
    
    ############### data quality checks #####################
    @dlt.expect("Ward Number valid","Ward > 0")
    @dlt.expect_or_fail("valid_dates_for_containment","Date_Identification < date_return_normal")
    def load_Data():
        return (
            spark.read.option("header", True)
            .schema(input_Schema)
            .csv(input_filename)
            .withColumnRenamed("BBMP_Zone", "zone")
            .withColumnRenamed("Ward_Number", "Ward")
            .withColumnRenamed("Ward_Name", "Ward_Name")
            .withColumnRenamed("Date_of_identification_as_Containment_zone", "Date_Identification")
            .withColumnRenamed("Total_Number_of_positive_cases", "No_Positive_Cases")
            .withColumnRenamed("Containment_Zone_ID", "zone_id")
            .withColumnRenamed("Date_of_return_to_normal", "date_return_normal")
        )
    return True
    

# COMMAND ----------

tables = ["containtment_banglore","course_sales"]
input_file = ["/mnt/raw/temp/rahulc/ContaintmentCorona_dataset.csv","/mnt/raw/temp/rahulc/SalesForCourse_quizz_table.csv"]
schema = [schema1,schema2]

for i in range (0,2):
    if i==0:
        status = read_rawdata(tables[i],input_file[i],schema[i])
#read_rawdata(tables[1],input_file[1],schema[1])

read_rawdata("containtment_banglore","/mnt/raw/temp/rahulc/ContaintmentCorona_dataset.csv",schema1)
read_rawdata("course_sales","/mnt/raw/temp/rahulc/SalesForCourse_quizz_table.csv",schema2)

# COMMAND ----------

# DBTITLE 1,Fetch status Extended
@dlt.table(
    comment="identification of valid dates",
    name="Containment_Extended"
)

def clean_data_containment():
    return dlt.read("containtment_banglore").filter("Status == 'EXTENDED'")

# COMMAND ----------

# DBTITLE 1,Sum of Positive cases whose Status is Extended
@dlt.table(
    comment="Get sum of total number of cases whose status is extended",
    name="containment_sum_cases_ext",
)
def get_sum_total_cases():
    return (
        dlt.read("Containment_Extended")
        .withColumn("No_Positive_Cases", expr("CAST(No_Positive_Cases AS INT)"))
        .groupBy("zone", "Ward_Name")
        .agg(sum("No_Positive_Cases").alias("Sum_Of_Extended_Cases"))
    )


# COMMAND ----------

# DBTITLE 1,Fetch status Active
@dlt.table(
    comment="identification of active cases",
    name="Containment_Active"
)

def clean_data_containment():
    return dlt.read("containtment_banglore").filter("Status == 'ACTIVE'")

# COMMAND ----------

# DBTITLE 1,Sum of Positive cases whose Status is Active
@dlt.table(
    comment="Get sum of total number of cases whose status is active",
    name="containment_sum_cases_act",
)
def get_sum_total_cases():
    return (
        dlt.read("Containment_Active")
        .withColumn("No_Positive_Cases", expr("CAST(No_Positive_Cases AS INT)"))
        .groupBy("zone", "Ward_Name")
        .agg(sum("No_Positive_Cases").alias("Sum_Of_Active_Cases"))
    )

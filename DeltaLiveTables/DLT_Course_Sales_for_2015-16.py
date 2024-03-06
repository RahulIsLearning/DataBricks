# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

# COMMAND ----------

# DBTITLE 1,Reading File with Schema
schema = (
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

df = (
    spark.read.option("header", True)
    .schema(schema)
    .csv("/mnt/raw/temp/rahulc/SalesForCourse_quizz_table.csv")
)

# dropping null values
resdf = df.na.drop()

# COMMAND ----------

# DBTITLE 1,Read raw data from File
@dlt.table(
    name="course_sales", 
    comment="Data set for sales of courses quiz",
    partition_cols=["Year", "Customer_Gender", "Country"]
)

#@dlt.except("valid timestamp","col('Year') IN ('2015','2016')")

def courseSalesRaw():
    return (
        resdf.withColumn("index", col("index").cast("int"))
        .withColumn("Year", col("Year").cast("int"))
        .withColumn("Customer_Age", col("Customer_Age").cast("int"))
        .withColumn("Quantity", col("Quantity").cast("int"))
        .select("index",
                "Date",
                "Year",
                "Month",
                "Customer_Age",
                "Customer_Gender",
                "Country","State",
                "Product_Category",
                "Sub_Category",
                "Quantity",
                "Unit_Cost",
                "Unit_Price",
                "Cost",
                "Revenue")
           )

# COMMAND ----------

# DBTITLE 1,Calculate avg for year 2015
@dlt.table(
    name="year2015_analysis",
    comment="split the data into year of 2015 and calculate avg of prices",
    partition_cols=["Customer_Gender", "Country"]
)

def courseYear2015():
    return (
        dlt.read("course_sales")
        .groupBy("Year","Customer_Age","Customer_Gender","Country","State","Product_Category")
        .agg(sum("Quantity").cast("int").alias("Quantity"), 
             avg("Unit_Cost").cast("int").alias("Avg_Unit_Cost"),
             avg("Unit_Price").cast("int").alias("Avg_Unit_Price"), 
             avg("Cost").cast("int").alias("Avg_Cost"), 
             sum("Revenue").cast("int").alias("Total_Revenue"))
        .filter(col("Year")==2015)
    )


# COMMAND ----------

# DBTITLE 1,Calculate avg for year 2016
@dlt.table(
    name="year2016_analysis",
    comment="split the data into year of 2016 and calculate avg of prices",
    partition_cols=["Customer_Gender", "Country"]
)

def courseYear2016():
    return (
        dlt.read("course_sales")
        .groupBy("Year","Customer_Age","Customer_Gender","Country","State","Product_Category")
        .agg(sum("Quantity").cast("int").alias("Quantity"), 
             avg("Unit_Cost").cast("int").alias("Avg_Unit_Cost"),
             avg("Unit_Price").cast("int").alias("Avg_Unit_Price"), 
             avg("Cost").cast("int").alias("Avg_Cost"), 
             sum("Revenue").cast("int").alias("Total_Revenue"))
        .filter(col("Year")==2016)
    )


# COMMAND ----------

# DBTITLE 1,Calculate avg for year 2015 for every gender
@dlt.table(
    name="year2015_gender_analysis",
    comment="split the data into year of 2015 and gender to calculate avg of prices"
)

def courseYearGender2015():
    return (
        dlt.read("year2015_analysis")
        .groupBy("Customer_Gender")
        .agg(sum("Quantity").cast("int").alias("Quantity"), 
             avg("Avg_Unit_Cost").cast("int").alias("Avg_Unit_Cost"),
             avg("Avg_Unit_Price").cast("int").alias("Avg_Unit_Price"), 
             avg("Avg_Cost").cast("int").alias("Avg_Cost"), 
             sum("Total_Revenue").cast("int").alias("Total_Revenue"))
    )


# COMMAND ----------

# DBTITLE 1,Calculate avg for year 2016 for every gender
@dlt.table(
    name="year2016_gender_analysis",
    comment="split the data into year of 2016 and gender to calculate avg of prices"
)

def courseYearGender2016():
    return (
        dlt.read("year2016_analysis")
        .groupBy("Customer_Gender")
        .agg(sum("Quantity").cast("int").alias("Quantity"), 
             avg("Avg_Unit_Cost").cast("int").alias("Avg_Unit_Cost"),
             avg("Avg_Unit_Price").cast("int").alias("Avg_Unit_Price"), 
             avg("Avg_Cost").cast("int").alias("Avg_Cost"), 
             sum("Total_Revenue").cast("int").alias("Total_Revenue"))
    )


# COMMAND ----------

# DBTITLE 1,Calculate avg for year 2015 for every country
@dlt.table(
    name = "year2015_country_analysis",
    comment = "split the data into year of 2015 and country"
)


def courseYearCountry2015():
    return (
        dlt.read("year2015_analysis")
        .groupBy("Country","State")
        .agg(sum("Quantity").cast("int").alias("Quantity"), 
             avg("Avg_Unit_Cost").cast("int").alias("Avg_Unit_Cost"),
             avg("Avg_Unit_Price").cast("int").alias("Avg_Unit_Price"), 
             avg("Avg_Cost").cast("int").alias("Avg_Cost"), 
             sum("Total_Revenue").cast("int").alias("Total_Revenue"))
    )

# COMMAND ----------

# DBTITLE 1,Calculate avg for year 2016 for every country
@dlt.table(
    name = "year2016_country_analysis",
    comment = "split the data into year of 2016 and country"
)


def courseYearCountry2015():
    return (
        dlt.read("year2016_analysis")
        .groupBy("Country","State")
        .agg(sum("Quantity").cast("int").alias("Quantity"), 
             avg("Avg_Unit_Cost").cast("int").alias("Avg_Unit_Cost"),
             avg("Avg_Unit_Price").cast("int").alias("Avg_Unit_Price"), 
             avg("Avg_Cost").cast("int").alias("Avg_Cost"), 
             sum("Total_Revenue").cast("int").alias("Total_Revenue"))
    )
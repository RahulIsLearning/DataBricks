# Databricks notebook source
# DBTITLE 1,Import libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

# COMMAND ----------

data = (("Bob", "IT", 4500), \
("Maria", "IT", 4600), \
("James", "IT", 3850), \
("Maria", "HR", 4500), \
("James", "IT", 4500), \
("Sam", "HR", 3300), \
("Jen", "HR", 3900), \
("Jeff", "Marketing", 4500), \
("Anand", "Marketing", 2000),\
("Shaid", "IT", 3850) \
)

schema= ["Name", "MBA_Stream", "SEM_MARKS"]

df = spark.createDataFrame(data,schema)

# COMMAND ----------

# DBTITLE 1,Row_Number()
w = Window.partitionBy("MBA_Stream").orderBy("Name")
df_rowNum = df.withColumn("Windowfunc_row",row_number().over(w))

display(df_rowNum)

# COMMAND ----------

# DBTITLE 1,Rank()
# This function is used to provide with the Rank of the given data frame. This is a window operation that is used to create the Rank from the Data Frame.
w = Window.partitionBy("MBA_Stream").orderBy("Name")
df_rank = df.withColumn("Window_Rank",rank().over(w))

display(df_rank)

# COMMAND ----------

# DBTITLE 1,Dense_Rank()
#Similar to Rank Function this is also used to rank elements but the difference being the ranking is without any gaps

w = Window.partitionBy("MBA_Stream").orderBy("Name")
df_denserank = df.withColumn("Window_Rank",dense_rank().over(w))

display(df_denserank)

# COMMAND ----------

# DBTITLE 1,Ntile()
# It returns the relative rank of the result, it has an argument value from where the ranking element will lie on.

w = Window.partitionBy("MBA_Stream").orderBy("Name")
df_ntile = df.withColumn("Window_Rank",ntile(2).over(w))

display(df_ntile)

# COMMAND ----------

# DBTITLE 1,LAG Function
# This is a window function used to access the previous data from the defined offset value.

w = Window.partitionBy("MBA_Stream").orderBy("Name")
df_lag = df.withColumn("Window_lag",lag("SEM_MARKS",1).over(w))

display(df_lag)


# COMMAND ----------

# DBTITLE 1, LEAD Function
# This is a window function used to access the next data from the defined offset value.

w = Window.partitionBy("MBA_Stream").orderBy("Name")
df_lead = df.withColumn("Window_lag",lead("SEM_MARKS",1).over(w))

display(df_lead)

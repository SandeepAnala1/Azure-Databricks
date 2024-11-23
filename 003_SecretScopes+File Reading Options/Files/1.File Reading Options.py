# Databricks notebook source
# MAGIC %md
# MAGIC ##RDD-- Reselient Distributed Data Set

# COMMAND ----------


# Create an RDD from a list
data = [1, 2, 3, 4, 5]
rdd = sc.parallelize(data)

# Use transformations and actions
squared_rdd = rdd.map(lambda x: x ** 2)

result = squared_rdd.reduce(lambda x, y: x + y)

# Print the result
print("Result:", result)

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType

schema_var = StructType([
    StructField("EmployeeID", IntegerType(), True),
    StructField("EmployeeName", StringType(), True),
    StructField("Address", StringType(), True),
    StructField("bad_record", StringType(), True),
])
df=spark.read.format('CSV').schema(schema_var).option('header',True).option("columnNameOfCorruptRecord","bad_record").load("/mnt/devmadeadlssa/raw/Employee.CSV")

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##PERMISSIVE

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema = StructType([
    StructField("EmployeeID", IntegerType(), True),
    StructField("EmployeeName", StringType(), True),
    StructField("Address", StringType(), True),   
    StructField("_corrupt_record", StringType(), True)
])

df_read=spark.read.format("csv").schema(schema).option("sep",",").option("header",True).option("path","/mnt/devmadeadlssa/raw/Employee.CSV").load()



# COMMAND ----------

df_read.show()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema = StructType([
    StructField("EmployeeID", IntegerType(), True),
    StructField("EmployeeName", StringType(), True),
    StructField("Address", StringType(), True),   
    StructField("bad_record", StringType(), True)
])

df_read=spark.read.format("csv").schema(schema).option("sep",",").option("header",True).option("columnNameOfCorruptRecord","bad_record").option("path","/mnt/devmadeadlssa/raw/Employee.CSV").load()


# COMMAND ----------

df_read.show()

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %md
# MAGIC ##FAILFAST

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema = StructType([
    StructField("EmployeeID", IntegerType(), True),
    StructField("EmployeeName", StringType(), True),
    StructField("Address", StringType(), True)
])
df_read=spark.read.format("csv").schema(schema).option("sep",",").option("header",True).option("mode","FAILFAST").option("path","/mnt/devmadeadlssa/raw/Employee.CSV").load()

# COMMAND ----------

df_read.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##DROPMALFORMED

# COMMAND ----------

df_read=spark.read.format("csv").schema(schema).option("sep",",").option("header",True).option("mode","DROPMALFORMED").option("path","/mnt/devmadeadlssa/raw/Employee.CSV").load()

# COMMAND ----------

df_read.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##badRecordsPath

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

schema = StructType([
    StructField("EmployeeID", IntegerType(), True),
    StructField("EmployeeName", StringType(), True),
    StructField("Address", StringType(), True)
])
df_read=spark.read.format("CSV").option("sep",",").schema(schema).option("header",True).option("badRecordsPath","/mnt/devmadeadlssa/error").load("/mnt/devmadeadlssa/raw/Employee.CSV")

df_read.show(truncate=False)


# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

df_read=spark.read.format('csv').option('sep',',').option('header',True).load("/mnt/devmadeadlssa/raw")

# COMMAND ----------

df_read.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Skip First N rows

# COMMAND ----------

df_read1=spark.read.format("CSV").option("sep",",").option("header",True).option("skipRows", 3).load("/mnt/devmadeadlssa/raw/Employee4.CSV")


# COMMAND ----------

df_read1.show()

# COMMAND ----------

df_read1.columns

# COMMAND ----------

df_read1.inputFiles()

# COMMAND ----------

df_read1.rdd.getNumPartitions()

# COMMAND ----------

df_input=spark.read.format('csv').option('sep',',').option('header',True).option("recursiveFileLookup", "true").load('/mnt/devmadeadlssa/raw/input1')

# COMMAND ----------

df_input.inputFiles()

# COMMAND ----------

df_input.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Recursive File Lookup

# COMMAND ----------

df_read1=spark.read.format("csv").schema(schema).option("sep",",").option("pathGlobFilter","*.CSV").option("recursiveFileLookup", "true").option("header",True).option("path","/mnt/devmadeadlssa/raw/").load()

df_read1.inputFiles()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Modification Time Path Filters

# COMMAND ----------

spark.conf.get('spark.sql.session.timeZone')

# COMMAND ----------

name="Ruby"
print(f"My Name is {name}")

# COMMAND ----------

v1="2024-03-31T14:22:30.424Z"
df_read=spark.read.format("CSV").option("sep",",").option("header",True).option("modifiedBefore", f"{v1}").load("/mnt/devmadeadlssa/raw/")

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_timestamp()

# COMMAND ----------

df_read.inputFiles()

# COMMAND ----------

spark.conf.get('spark.sql.session.timeZone')

# COMMAND ----------

df_read.show()

# COMMAND ----------



# Databricks notebook source
# MAGIC %md
# MAGIC ## Narrow Transformation

# COMMAND ----------

# MAGIC %md
# MAGIC #Select

# COMMAND ----------

data = [("Alice", 25, "New York"),
        ("Bob", 30, "San Francisco"),
        ("Charlie", 28, "Los Angeles"),
        ("Ashok", 38, "San Diego") ]

columns = ["name", "age", "city"]

df = spark.createDataFrame(data, columns)


# COMMAND ----------

df.show()

# COMMAND ----------

df_select=df.select('*')
df_select.show()



# COMMAND ----------

df_select1=df.select('name','age')
df_select1.show()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

selected_df2= df.select(col("name"),col("city"))

# COMMAND ----------

selected_df3=df.select(df["name"],df["city"])


# COMMAND ----------

select e.name from employee e

# COMMAND ----------


selected_df4=df.select(df.name,df.city)

# COMMAND ----------

# MAGIC %md
# MAGIC #2.SelectExpr

# COMMAND ----------

     # Sample data as a DataFrame
data = [("Alice", 25, "New York"),
        ("Bob", 30, "San Francisco"),
        ("Charlie", 28, "Los Angeles")]

columns = ["name", "age", "city"]

df = spark.createDataFrame(data, columns)


# COMMAND ----------

selected_df = df.selectExpr("name", "age + 1 as age_plus_one", "upper(city) as upper_city")

# COMMAND ----------

selected_df.show()

# COMMAND ----------

from pyspark.sql.functions import expr

# COMMAND ----------

selected_df1= df.select("name",expr("age+1 as age_plusone"))

# COMMAND ----------

selected_df1.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #3. Filter

# COMMAND ----------

# Sample data as a DataFrame
data = [("Alice", 25, "New York"),
        ("Bob", 30, "San Francisco"),
        ("Charlie", 28, "Los Angeles"),
        ("David", 35, "Chicago"),
        ("Eve", 22, "New York")]

columns = ["name", "age", "city"]

df = spark.createDataFrame(data, columns)


# COMMAND ----------

filtered_df = df.filter(df["age"] < 30  )

# COMMAND ----------

filtered_df.show()

# COMMAND ----------

filtered_df1 = df.filter("age= 30 or name!='Ashok'")

# COMMAND ----------

filtered_df1.show()

# COMMAND ----------

filtered_df2 = df.filter((df["age"] == 30) | (df["name"] != 'Ashok'))

# COMMAND ----------

filtered_df2.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #4. drop 

# COMMAND ----------

from pyspark.sql.functions import *
# Sample data as a DataFrame
data = [("Alice", 25, "New York"),
        ("Bob", 30, "San Francisco"),
        ("Charlie", 28, "Los Angeles"),
        ("David", 35, "Chicago"),
        ("Eve", 22, "New York")]

columns = ["name", "age", "city"]


# COMMAND ----------

df=df.drop('city','age')

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #dropDuplicates

# COMMAND ----------

data = [(1, 101, 201, "2023-01-15"),
  (2, 102, 202, "2023-01-16"),
  (3, 101, 201, "2023-01-17"),
  (4, 103, 203, "2023-01-18")]
columns = ["OrderID", "CustomerID", "ProductID", "OrderDate"]
orders_df = spark.createDataFrame(data, columns)


# COMMAND ----------

orders_df.show()

# COMMAND ----------

orders_df.dropDuplicates(["CustomerID","ProductID"]).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC # Fillna

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

schema=StructType(fields=[StructField("transactionId",IntegerType(),True),StructField("predError",IntegerType(),True),StructField("value",IntegerType(),True),StructField("storeId",IntegerType(),True),StructField("productId",IntegerType(),True), StructField("f",IntegerType(),True)])

py_list = [
    (1, 3, 4, 25, 1, None),
    (2, 6, 7, 2, 2, None),
    (3, 3, None, 25, 3, None),
    (4, None, None, 3, 2, None),
    (5, None, None, None, 2, None),
    (6, 3, 2, 25, 2, None)
]

df_new_list=spark.createDataFrame(py_list,schema)



# COMMAND ----------

df_new_list.show()

# COMMAND ----------

df1=df_new_list.fillna(0)

# COMMAND ----------

df1.show()

# COMMAND ----------

df_new_list_na=df_new_list.fillna(0,subset=["value","storeId"])

# COMMAND ----------

df_new_list_na.show()

# COMMAND ----------

# MAGIC %md
# MAGIC # dropna

# COMMAND ----------

schema=StructType(fields=[StructField("transactionId",IntegerType(),True),StructField("predError",IntegerType(),True),StructField("value",IntegerType(),True),StructField("storeId",IntegerType(),True),StructField("productId",IntegerType(),True), StructField("f",IntegerType(),True)])

py_list = [
    (1, 3, 4, 25, 1, None),
    (2, 6, 7, 2, 2, None),
    (3, 3, None, 25, 3, None),
    (4, None, None, 3, 2, None),
    (5, None, None, None, 2, None),
    (6, 3, 2, 25, 2, None)
]
df_new_list=spark.createDataFrame(py_list,schema)


# COMMAND ----------

df_new_list.show()

# COMMAND ----------

df_new=df_new_list.drop('f')

# COMMAND ----------

df_drop=df_new.dropna()
df_drop.show(truncate=False)


# COMMAND ----------

df_drop1=df_new_list.dropna(subset=["predError","storeId"])
df_drop1.show(truncate=False)


# COMMAND ----------

df_drop2=df_new_list.dropna(thresh=4)
df_drop2.show(truncate=False)


# COMMAND ----------

# MAGIC %md
# MAGIC #WithColumn
# MAGIC
# MAGIC

# COMMAND ----------

   from pyspark.sql.functions import *
data = [(1, 101, 201, "2023-01-15"),
  (2, 102, 202, "2023-01-16"),
  (3, 101, 201, "2023-01-17"),
  (4, 103, 203, "2023-01-18")]
columns = ["OrderID", "CustomerID", "ProductID", "OrderDate"]

# COMMAND ----------

df=spark.createDataFrame(data,columns)

# COMMAND ----------

df_new=df.withColumn('ingested_date',current_timestamp())\
    .withColumn('FileName',lit('Test.csv'))

# COMMAND ----------

df_new.show()

# COMMAND ----------

df_new.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #withColumnRenamed

# COMMAND ----------

from pyspark.sql.functions import *
data = [(1, 101, 201, "2023-01-15"),
  (2, 102, 202, "2023-01-16"),
  (3, 101, 201, "2023-01-17"),
  (4, 103, 203, "2023-01-18")]
columns = ["OrderID", "CustomerID", "ProductID", "OrderDate"]
orders_df = spark.createDataFrame(data, columns)



# COMMAND ----------

df_new=orders_df.withColumnRenamed('OrderID','Order_ID')\
    .withColumnRenamed('CustomerID','customer_id')

# COMMAND ----------

df_new.show()

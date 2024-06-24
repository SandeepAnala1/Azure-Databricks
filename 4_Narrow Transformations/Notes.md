# Index
- [Magic commands] (https://github.com/SandeepAnala1/Azure-Databricks/edit/main/4_Narrow%20Transformations/Notes.md#magic-commands)
- [Creation of dataframes from static data]()
- [Select in pyspark]()
- [Select Expression]()
- [Filter Transformation]()
- [Drop]()
- [dropDuplicates]()
- [Fillna]()
- [Dropna]()
- [WithColumn]()
- [withColumnRenamed]()


# Magic commands
- If you are writing a python code and wanted to execute a sql code, you can perform below code and perform <br>

  ```sql
  %sql
  select xxxxxxxx
  ```
- It can be applicable for python, sql, r, md.

# Creation of dataframes from static data
- dataframes is combination of 2 things, data and column
- You store data in the form of tuples inside the list
  
![image](https://github.com/SandeepAnala1/Azure-Databricks/assets/163712602/f3025a98-b75f-4cea-bba0-55e7ce625da8)

- It also infers the data

# Select in pyspark
We can apply this parallelly on the data at a time
```python
df_select = df.select('name','city').show()
```
Other ways of writing it: To perform below code we need to import col from pyspark.sql.functions
```python
from pyspark.sql.functions import col
selected_df2= df.select(col("name"),col("city"))
selected_df3=df.select(df["name"],df["city"])
selected_df4=df.select(df.name,df.city)
```
-----------------------------

# Select Expression
If you want to modify the column names while modifying the values, this can be used

```python
selected_df = df.selectExpr("name", "age + 1 as age_plus_one", "upper(city) as upper_city")
selected_df.show
```

| Name   | Age Plus One | Upper City    |
|--------|--------------|---------------|
| Alice  | 26           | NEW YORK      |
| Bob    | 31           | SAN FRANCISCO |
| Charlie| 29           | LOS ANGELES   |

- We can perform the same with select also, but we need to import expr function
```python
from pyspark.sql.functions import expr
selected_df1= df.select("name",expr("age+1 as age_plusone"))
```

----------------------------

# Filter Transformation
It's like select queries in SQL, here we can use filter or where

```python
# Sample data as a DataFrame
data = [("Alice", 25, "New York"),
        ("Bob", 30, "San Francisco"),
        ("Charlie", 28, "Los Angeles"),
        ("David", 35, "Chicago"),
        ("Eve", 22, "New York")]
columns = ["name", "age", "city"]
df = spark.createDataFrame(data, columns)
filtered_df = df.filter(df["age"] < 30  ) #df.where(df["age"] < 30  )
filtered_df.show()
```
| Name   | Age | City        |
|--------|-----|-------------|
| Alice  | 25  | New York    |
| Charlie| 28  | Los Angeles |
| Eve    | 22  | New York    |

- with conditions and other ways
```python
filtered_df1 = df.filter("age= 30 or name!='Ashok'")
filtered_df2 = df.filter((df["age"] == 30) | (df["name"] != 'Ashok'))
```

-------------------------------

# Drop
If I have 20 columns and I have to drop one specific column

```python
df=df.drop('city','age')
df.show()
```

| Name   |
|--------|
| Alice  |
| Bob    |
| Charlie|
| David  |
| Eve    |

--------------------------

# dropDuplicates -- It's a wide transformation
Drops duplicates in dataframe

```python
data = [(1, 101, 201, "2023-01-15"),
  (2, 102, 202, "2023-01-16"),
  (3, 101, 201, "2023-01-17"),
  (4, 103, 203, "2023-01-18")]
columns = ["OrderID", "CustomerID", "ProductID", "OrderDate"]
orders_df = spark.createDataFrame(data, columns)
orders_df.show()
```
| OrderID | CustomerID | ProductID | OrderDate  |
|---------|------------|-----------|------------|
| 1       | 101        | 201       | 2023-01-15 |
| 2       | 102        | 202       | 2023-01-16 |
| 3       | 101        | 201       | 2023-01-17 |
| 4       | 103        | 203       | 2023-01-18 |

```python
orders_df.dropDuplicates(["CustomerID","ProductID"]).show(truncate=False)
```
| OrderID | CustomerID | ProductID | OrderDate  |
|---------|------------|-----------|------------|
| 1       | 101        | 201       | 2023-01-15 |
| 2       | 102        | 202       | 2023-01-16 |
| 4       | 103        | 203       | 2023-01-18 |

----------------------------------------

# Fillna
Filling Null values with other values
- Instead of Databricks inferring schema I have given the schema

```python
from pyspark.sql.types import *
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
df_new_list.show()
```
| transactionId | predError | value | storeId | productId | f   |
|---------------|-----------|-------|---------|-----------|-----|
| 1             | 3         | 4     | 25      | 1         | NULL|
| 2             | 6         | 7     | 2       | 2         | NULL|
| 3             | 3         | NULL  | 25      | 3         | NULL|
| 4             | NULL      | NULL  | 3       | 2         | NULL|
| 5             | NULL      | NULL  | NULL    | 2         | NULL|
| 6             | 3         | 2     | 25      | 2         | NULL|

```python
df1=df_new_list.fillna(0)
df1.show()
```
| transactionId | predError | value | storeId | productId | f |
|---------------|-----------|-------|---------|-----------|---|
| 1             | 3         | 4     | 25      | 1         | 0 |
| 2             | 6         | 7     | 2       | 2         | 0 |
| 3             | 3         | 0     | 25      | 3         | 0 |
| 4             | 0         | 0     | 3       | 2         | 0 |
| 5             | 0         | 0     | 0       | 2         | 0 |
| 6             | 3         | 2     | 25      | 2         | 0 |


---------------------------

# Dropna
Drop Null values

```python
df_drop=df_new.dropna()
df_drop.show(truncate=False)
```

```python
df_drop1=df_new_list.dropna(subset=["predError","storeId"])
df_drop1.show(truncate=False)
```
- It gives you the count of not null columns
```python
df_drop2=df_new_list.dropna(thresh=4)
df_drop2.show(truncate=False)
```

-----------------------------

# WithColumn
The withColumn function in PySpark DataFrame is used to add, update, or rename a column in a DataFrame

```python
from pyspark.sql.functions import *
data = [(1, 101, 201, "2023-01-15"),
  (2, 102, 202, "2023-01-16"),
  (3, 101, 201, "2023-01-17"),
  (4, 103, 203, "2023-01-18")]
columns = ["OrderID", "CustomerID", "ProductID", "OrderDate"]
df=spark.createDataFrame(data,columns)
df_new=df.withColumn('ingested_date',current_timestamp())\
    .withColumn('FileName',lit('Test.csv'))
df_new.show()
```
| OrderID | CustomerID | ProductID | OrderDate  | ingested_date             | FileName |
|---------|------------|-----------|------------|---------------------------|----------|
| 1       | 101        | 201       | 2023-01-15 | 2024-06-24 12:30:00.123456| Test.csv |
| 2       | 102        | 202       | 2023-01-16 | 2024-06-24 12:30:00.123456| Test.csv |
| 3       | 101        | 201       | 2023-01-17 | 2024-06-24 12:30:00.123456| Test.csv |
| 4       | 103        | 203       | 2023-01-18 | 2024-06-24 12:30:00.123456| Test.csv |


------------------------------

# withColumnRenamed
Rename the column
```python
from pyspark.sql.functions import *
data = [(1, 101, 201, "2023-01-15"),
  (2, 102, 202, "2023-01-16"),
  (3, 101, 201, "2023-01-17"),
  (4, 103, 203, "2023-01-18")]
columns = ["OrderID", "CustomerID", "ProductID", "OrderDate"]
orders_df = spark.createDataFrame(data, columns)
df_new=orders_df.withColumnRenamed('OrderID','Order_ID')\
    .withColumnRenamed('CustomerID','customer_id')
df_new.show()
```
| Order_ID | customer_id | ProductID | OrderDate  |
|----------|-------------|-----------|------------|
| 1        | 101         | 201       | 2023-01-15 |
| 2        | 102         | 202       | 2023-01-16 |
| 3        | 101         | 201       | 2023-01-17 |
| 4        | 103         | 203       | 2023-01-18 |










  
   

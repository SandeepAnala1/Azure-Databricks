In the context of distributed computing frameworks like Apache Spark, transformations are operations that create a new RDD (Resilient Distributed Dataset) from an existing one.

# Wide Transformations

Wide transformations (also known as shuffle transformations) are operations that result in data being shuffled between different nodes in the cluster. This involves repartitioning the data across the cluster and can be more expensive and time-consuming than narrow transformations.

# Groupby

In PySpark, `groupBy` is a transformation operation used to group the DataFrame rows by a specified column or columns and then perform aggregation functions on them. 

```Python
# Read data into a DataFrame
data = [("Alice", "Sales", 4000),
        ("Bob", "HR", 5000),
        ("Carol", "Sales", 6000),
        ("Dave", "IT", 5500),
        ("Eve", "HR", 6000)]

columns = ["Name", "Department", "Salary"]
df = spark.createDataFrame(data,columns)
df.show(truncate=False)
```
| Name  | Department | Salary |
|-------|------------|--------|
| Alice | Sales      | 4000   |
| Bob   | HR         | 5000   |
| Carol | Sales      | 6000   |
| Dave  | IT         | 5500   |
| Eve   | HR         | 6000   |


```python
gr = df.groupBy("Department").sum('Salary')
gr.show()
```

| Department | sum(Salary) |
|------------|-------------|
| Sales      | 10000       |
| HR         | 11000       |
| IT         | 5500        |

-------------------------------------------

# Pivot
- Transforming rows into columns




--------------------------------

# Unpivot

Sure! Below is an example of how you can perform an unpivot operation in PySpark using PySpark DataFrame functions.

### Example: Unpivot in PySpark

Assume you have a DataFrame `df` in PySpark that looks like this:

| id | category_A | category_B | category_C |
|----|------------|------------|------------|
| 1  | 10         | 20         | 30         |
| 2  | 40         | 50         | 60         |
| 3  | 70         | 80         | 90         |

You want to unpivot this DataFrame to have the following structure:

| id | category | value |
|----|----------|-------|
| 1  | A        | 10    |
| 1  | B        | 20    |
| 1  | C        | 30    |
| 2  | A        | 40    |
| 2  | B        | 50    |
| 2  | C        | 60    |
| 3  | A        | 70    |
| 3  | B        | 80    |
| 3  | C        | 90    |

### Steps to Unpivot in PySpark

1. **Initialize SparkSession**:
    ```python
    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
        .appName("Unpivot Example") \
        .getOrCreate()
    ```

2. **Create the DataFrame**:
    ```python
    from pyspark.sql import Row
    data = [
        Row(id=1, category_A=10, category_B=20, category_C=30),
        Row(id=2, category_A=40, category_B=50, category_C=60),
        Row(id=3, category_A=70, category_B=80, category_C=90)
    ]
    df = spark.createDataFrame(data)
    ```

3. **Unpivot the DataFrame**:
    ```python
    from pyspark.sql.functions import expr
    
    unpivotExpr = "stack(3, 'A', category_A, 'B', category_B, 'C', category_C) as (category, value)"
    unpivoted_df = df.select("id", expr(unpivotExpr))
    ```

4. **Show the Result**:
    ```python
    unpivoted_df.show()
    ```

### Full Code Example

Here is the complete code for the unpivot operation:

```python
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import expr

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Unpivot Example") \
    .getOrCreate()

# Create the DataFrame
data = [
    Row(id=1, category_A=10, category_B=20, category_C=30),
    Row(id=2, category_A=40, category_B=50, category_C=60),
    Row(id=3, category_A=70, category_B=80, category_C=90)
]
df = spark.createDataFrame(data)

# Unpivot the DataFrame
unpivotExpr = "stack(3, 'A', category_A, 'B', category_B, 'C', category_C) as (category, value)"
unpivoted_df = df.select("id", expr(unpivotExpr))

# Show the result
unpivoted_df.show()
```

### Output
The resulting DataFrame will look like this:

| id | category | value |
|----|----------|-------|
| 1  | A        | 10    |
| 1  | B        | 20    |
| 1  | C        | 30    |
| 2  | A        | 40    |
| 2  | B        | 50    |
| 2  | C        | 60    |
| 3  | A        | 70    |
| 3  | B        | 80    |
| 3  | C        | 90    |

This code effectively unpivots the DataFrame by converting the wide format into a long format using the `stack` function in PySpark.





--------------------------------

# Window Function
- When to use window function ?

Window functions and aggregate functions are both powerful tools for data analysis in SQL and PySpark, but they serve different purposes and are used in different scenarios. Here's a comparison to help you understand when to use each:

### Aggregate Functions

**Purpose**: Aggregate functions are used to compute a single result from a set of input values. They operate on a group of rows and return a single value for each group.

**Common Use Cases**:
1. **Summarizing Data**: Calculating totals, averages, counts, etc., for groups of rows.
2. **Group By Operations**: When you want to summarize data at a group level.

### Window Functions

**Purpose**: Window functions allow you to perform calculations across a set of table rows that are somehow related to the current row. Unlike aggregate functions, window functions do not cause rows to be grouped into a single output row; instead, the rows retain their separate identities.

**Common Use Cases**:
1. **Ranking**: Assigning ranks to rows.
2. **Running Totals**: Calculating cumulative sums or other aggregates over a window of rows.
3. **Moving Averages**: Calculating averages over a sliding window.
4. **Row Comparisons**: Comparing values between rows (e.g., difference between current and previous row).

Certainly! Below is a code snippet to create a DataFrame in PySpark with sample data and schema, and then demonstrate the usage of various window functions.

### Sample Data and Schema
Let's create a sample DataFrame with the following data:

| Name  | Date       | Value |
|-------|------------|-------|
| Alice | 2021-01-01 | 10    |
| Alice | 2021-01-02 | 20    |
| Alice | 2021-01-03 | 15    |
| Bob   | 2021-01-01 | 30    |
| Bob   | 2021-01-02 | 25    |
| Bob   | 2021-01-03 | 35    |

### Creating the DataFrame

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, rank, dense_rank, lag, lead, sum as Fsum, avg as Favg
from pyspark.sql.window import Window

# Assuming Spark session is already created in Databricks
spark = SparkSession.builder.appName("WindowFunctionsExample").getOrCreate()

# Sample data
data = [
    ("Alice", "2021-01-01", 10),
    ("Alice", "2021-01-02", 20),
    ("Alice", "2021-01-03", 15),
    ("Bob", "2021-01-01", 30),
    ("Bob", "2021-01-02", 25),
    ("Bob", "2021-01-03", 35)
]

# Schema
schema = ["Name", "Date", "Value"]

# Creating DataFrame
df = spark.createDataFrame(data, schema)

# Displaying the DataFrame
df.show()
```

### Using Window Functions

1. **Row Number**
2. **Rank**
3. **Dense Rank**
4. **Lag and Lead**
5. **Cumulative Sum**
6. **Moving Average**

```python
# Define window specification
windowSpec = Window.partitionBy("Name").orderBy("Date")

# 1. Row Number
df.withColumn("row_number", row_number().over(windowSpec)).show()

# 2. Rank
df.withColumn("rank", rank().over(windowSpec)).show()

# 3. Dense Rank
df.withColumn("dense_rank", dense_rank().over(windowSpec)).show()

# 4. Lag and Lead
df.withColumn("lag", lag("Value", 1).over(windowSpec))\
  .withColumn("lead", lead("Value", 1).over(windowSpec)).show()

# 5. Cumulative Sum
df.withColumn("cumulative_sum", Fsum("Value").over(windowSpec)).show()

# 6. Moving Average (last 2 values)
windowSpecRows = windowSpec.rowsBetween(-1, 0)
df.withColumn("moving_avg", Favg("Value").over(windowSpecRows)).show()
```

------------------------------------------------------------

# Action
- Actions are operations that trigger the execution of transformations and return results to the driver program or write data to external storage.

# Collect
The `collect` action in PySpark is used to retrieve the entire dataset from a distributed DataFrame (or RDD) to the driver node as a list. It’s useful for small datasets, but should be used with caution on large datasets to avoid memory overflow on the driver node.

Here's an example using sample data to demonstrate the `collect` action:

### Sample Data
Let's create a simple PySpark DataFrame to work with:

```python
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("collect_example").getOrCreate()

# Sample data
data = [
    ("Alice", 34),
    ("Bob", 45),
    ("Cathy", 29),
    ("David", 40)
]

# Create DataFrame
df = spark.createDataFrame(data, ["Name", "Age"])

# Show the DataFrame
df.show()
```

This will output:

```
+-----+---+
| Name|Age|
+-----+---+
|Alice| 34|
|  Bob| 45|
|Cathy| 29|
|David| 40|
+-----+---+
```

### Using `collect`
To collect the data from the DataFrame to the driver node:

```python
# Collect the data to the driver
collected_data = df.collect()

# Print the collected data
for row in collected_data:
    print(f"Name: {row['Name']}, Age: {row['Age']}")
```

output:
```
Name: Alice, Age: 34
Name: Bob, Age: 45
Name: Cathy, Age: 29
Name: David, Age: 40
```
---------------

# First

The `first` action in PySpark returns the first element of the DataFrame (or RDD). It's a quick way to look at the first row of your data. Here’s how you can use the `first` action with a sample DataFrame.

### Step 1: Creating Sample Data
Next, we create some sample data and convert it into a DataFrame.

```python
# Sample data
data = [
    ("Alice", 34),
    ("Bob", 45),
    ("Cathy", 29),
    ("David", 40)
]

# Create DataFrame
df = spark.createDataFrame(data, ["Name", "Age"])

# Show the DataFrame
df.show()
```

This will output:

```
+-----+---+
| Name|Age|
+-----+---+
|Alice| 34|
|  Bob| 45|
|Cathy| 29|
|David| 40|
+-----+---+
```

### Step 3: Using `first` Action
To retrieve the first row of the DataFrame:

```python
# Get the first row
first_row = df.first()

# Print the first row
print(f"Name: {first_row['Name']}, Age: {first_row['Age']}")
```
Output:

```
Name: Alice, Age: 34
```

---------------------------------

# take(n)

In PySpark, `take(n)` is an action that retrieves the first `n` elements from a DataFrame or RDD (Resilient Distributed Dataset). Here's a brief example using sample data:

### Sample Data
Let's create a simple PySpark DataFrame with some sample data:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create a Spark session
spark = SparkSession.builder \
    .appName("Take Action Example") \
    .getOrCreate()

# Sample data
data = [("John", 25),
        ("Alice", 30),
        ("Bob", 28),
        ("Eve", 22)]

# Define schema
schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True)
])

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show DataFrame
df.show()
```

Output:
```
+-----+---+
| Name|Age|
+-----+---+
| John| 25|
|Alice| 30|
|  Bob| 28|
|  Eve| 22|
+-----+---+
```

### Using `take(n)` Action
Now, let's use the `take(n)` action to retrieve the first 2 rows from the DataFrame:

```python
# Take action
first_two_rows = df.take(2)

# Print the result
for row in first_two_rows:
    print(row)
```

Output:
```
Row(Name='John', Age=25)
Row(Name='Alice', Age=30)
```

### Explanation
- `take(n)` is used to extract `n` elements from the DataFrame.
- In this example, `df.take(2)` retrieves the first two rows from the DataFrame `df`.
- Each row is represented as a `Row` object, which can be accessed by column name (`row['Name']`, `row['Age']`) or index (`row[0]`, `row[1]`).

This action is useful for quickly inspecting or processing a small subset of data from a larger dataset in PySpark.

----------------------------

# head(n)

In PySpark, the `head(n)` action is similar to `take(n)` but is typically used on DataFrames to retrieve the first `n` rows. Here's how it works with sample data:

### Sample Data
Let's reuse the previous example with a PySpark DataFrame:

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create a Spark session
spark = SparkSession.builder \
    .appName("Head Action Example") \
    .getOrCreate()

# Sample data
data = [("John", 25),
        ("Alice", 30),
        ("Bob", 28),
        ("Eve", 22)]

# Define schema
schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True)
])

# Create DataFrame
df = spark.createDataFrame(data, schema)

# Show DataFrame
df.show()
```

Output:
```
+-----+---+
| Name|Age|
+-----+---+
| John| 25|
|Alice| 30|
|  Bob| 28|
|  Eve| 22|
+-----+---+
```

### Using `head(n)` Action
Now, let's use the `head(n)` action to retrieve the first 2 rows from the DataFrame:

```python
# Head action
first_two_rows = df.head(2)

# Print the result
for row in first_two_rows:
    print(row)
```

Output:
```
Row(Name='John', Age=25)
Row(Name='Alice', Age=30)
```

### Explanation
- `head(n)` retrieves the first `n` rows from the DataFrame.
- Unlike `take(n)`, `head(n)` is a method specifically available on DataFrames in PySpark.
- The result of `head(n)` is returned as a list of `Row` objects.
- Each `Row` object allows access to its columns either by name (`row['Name']`, `row['Age']`) or by index (`row[0]`, `row[1]`).

This action is useful for quickly examining the initial rows of a DataFrame to understand its structure and content.

--------------------------------

# Actions one liners

1. **`collect()`**: Returns all the elements of the DataFrame as an array to the driver program.
2. **`show()`**: Displays the top `n` rows of the DataFrame.
3. **`take(n)`**: Returns the first `n` rows as an array.
4. **`count()`**: Returns the number of rows in the DataFrame.
5. **`first()`**: Returns the first row of the DataFrame.
6. **`head(n)`**: Returns the first `n` rows as an array (similar to `take(n)`).
7. **`foreach(f)`**: Applies a function `f` to each element of the DataFrame.
8. **`reduce(f)`**: Aggregates the elements of the DataFrame using the specified binary function `f`.
9. **`saveAsTable()`**: Writes the DataFrame to a Hive table.
10. **`write.format(...).save()`**: Writes the DataFrame to a specified storage format.

### Sample Codes

Let's illustrate some of these actions using the DataFrame we created earlier:

```python
# Collect all elements
all_data = df.collect()
print("Collect:", all_data)

# Show top 5 rows
df.show(5)

# Take the first 3 rows
first_3 = df.take(3)
print("Take 3:", first_3)

# Count the number of rows
row_count = df.count()
print("Row count:", row_count)

# Get the first row
first_row = df.first()
print("First row:", first_row)

# Head function
head_3 = df.head(3)
print("Head 3:", head_3)

# Foreach (example: printing each row)
def print_row(row):
    print(row)
df.foreach(print_row)

# Reduce (example: summing the 'Value' column)
from pyspark.sql import Row
total_value = df.rdd.map(lambda row: row['Value']).reduce(lambda x, y: x + y)
print("Total Value:", total_value)

# Save as a table
df.write.saveAsTable("example_table")

# Save in a specific format (e.g., CSV)
df.write.format("csv").save("/path/to/save/csv")
```



















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

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






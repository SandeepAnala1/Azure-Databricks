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

















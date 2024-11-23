# How to write the data back to storage account
The line `df.write.mode().format().option().save('/mnt/devmadeadlssa/raw')` is a chain of methods used in PySpark for saving a DataFrame to storage. Here's a breakdown of each part:

1. **`df.write`**: This initiates the DataFrameWriter, which is an interface to specify how data should be written out.

2. **`.mode()`**: Specifies the mode for saving the DataFrame. Common modes include:
    - **`'append'`**: Adds the data to the existing data.
    - **`'overwrite'`**: Overwrites the existing data.
    - **`'ignore'`**: Ignores the operation if data already exists.
    - **`'error'` or `'errorifexists'`**: Throws an error if data already exists (this is the default mode).

    Example: 
    ```python
    df.write.mode('overwrite')
    ```

3. **`.format()`**: Specifies the format in which to save the DataFrame. Common formats include:
    - **`'parquet'`**: A columnar storage file format.
    - **`'csv'`**: Comma-separated values.
    - **`'json'`**: JavaScript Object Notation.
    - **`'orc'`**: Optimized Row Columnar format.

    Example: 
    ```python
    df.write.format('parquet')
    ```

4. **`.option()`**: Used to specify various options for the output format. This can include things like compression, delimiter (for CSV), and more.

    Example: 
    ```python
    df.write.option('compression', 'snappy')
    ```

5. **`.save('/mnt/devmadeadlssa/raw')`**: This is the path where the DataFrame will be saved. The path can be a local directory, a distributed file system like HDFS, or a cloud storage location like S3.

Putting it all together, here's an example with all parts specified:

```python
df.write.mode('overwrite').format('parquet').option('compression', 'snappy').save('/mnt/devmadeadlssa/raw')
```

In this example:
- **Mode**: `overwrite` (will overwrite existing data if any).
- **Format**: `parquet` (saves the DataFrame in Parquet format).
- **Option**: `compression`, `snappy` (uses Snappy compression).
- **Save Path**: `/mnt/devmadeadlssa/raw` (the location where the data will be saved).

This chain of methods effectively tells PySpark to save the DataFrame `df` as a Parquet file, overwriting any existing data at the specified path, and to use Snappy compression.

# Modes of Write
Here are the different modes for writing data using PySpark's DataFrameWriter, along with explanations for each:

1. **Overwrite**:
   - This mode overwrites the existing data at the specified path. If any data or files exist at that location, they will be replaced by the new data.
   - Example:
     ```python
     df.write.mode('overwrite').save('/path/to/save')
     ```

2. **Append**:
   - This mode appends the new data to the existing data at the specified path. It does not delete or replace any existing data but adds the new data to the end.
   - Example:
     ```python
     df.write.mode('append').save('/path/to/save')
     ```

3. **Ignore**:
   - This mode ignores the write operation if data already exists at the specified path. If the path is empty or does not exist, it writes the data; otherwise, it does nothing.
   - Example:
     ```python
     df.write.mode('ignore').save('/path/to/save')
     ```

4. **ErrorIfExists**:
   - This mode throws an error if data already exists at the specified path. It ensures that you do not accidentally overwrite existing data. This is the default mode if no mode is specified.
   - Example:
     ```python
     df.write.mode('errorifexists').save('/path/to/save')
     ```

### Example Usage

Here's an example showing how to use these modes:

```python
from pyspark.sql import SparkSession

# Create a Spark session
spark = SparkSession.builder.appName("PySparkWriteModes").getOrCreate()

# Sample DataFrame
data = [("James", "Sales", 3000), ("Michael", "Sales", 4600), 
        ("Robert", "Sales", 4100), ("Maria", "Finance", 3000), 
        ("Raman", "Finance", 3000)]

columns = ["Name", "Department", "Salary"]

df = spark.createDataFrame(data, schema=columns)

# Overwrite mode
df.write.mode('overwrite').format('parquet').save('/mnt/devmadeadlssa/overwrite')

# Append mode
df.write.mode('append').format('parquet').save('/mnt/devmadeadlssa/append')

# Ignore mode
df.write.mode('ignore').format('parquet').save('/mnt/devmadeadlssa/ignore')

# ErrorIfExists mode
df.write.mode('errorifexists').format('parquet').save('/mnt/devmadeadlssa/errorifexists')

# Stop the Spark session
spark.stop()
```

In this example:
- Data is saved in Parquet format.
- Different modes (`overwrite`, `append`, `ignore`, and `errorifexists`) are used to handle existing data differently.

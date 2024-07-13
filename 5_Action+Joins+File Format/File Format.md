# File format

Sure! Here are explanations and examples of various file formats you can use with PySpark for reading and writing data:

### 1. Delta
- **Description**: Delta Lake is an open-source storage layer that brings ACID transactions to Apache Spark and big data workloads. Delta Lake runs on top of your existing data lake and is fully compatible with Apache Spark APIs.
- **Usage Example**:
  ```python
  # Writing DataFrame to Delta format
  df.write.format("delta").save("/mnt/devmadeadlssa/delta")

  # Reading DataFrame from Delta format
  delta_df = spark.read.format("delta").load("/mnt/devmadeadlssa/delta")
  ```

### 2. Parquet
- **Description**: Parquet is a columnar storage file format optimized for use with big data processing frameworks. It provides efficient data compression and encoding schemes.
- **Usage Example**:
  ```python
  # Writing DataFrame to Parquet format
  df.write.format("parquet").save("/mnt/devmadeadlssa/parquet")

  # Reading DataFrame from Parquet format
  parquet_df = spark.read.format("parquet").load("/mnt/devmadeadlssa/parquet")
  ```
![image](https://github.com/user-attachments/assets/bcf6c3d6-36a0-4d30-9dc7-cd0d28b79a98)

  Parquet files contain metadata. This metadata plays a crucial role in the efficiency and optimization that Parquet provides. Here's an overview of the types of metadata stored in a Parquet file:
  
  ### Metadata in Parquet Files
  
  1. **File Metadata (Footer)**:
     - The footer of a Parquet file contains important information about the data stored in the file.
     - It includes schema information, row group information, and column chunk metadata.
     - The footer is read entirely into memory when a Parquet file is opened, allowing for efficient querying and data retrieval.
  
  2. **Schema**:
     - Parquet files store the schema of the data, which includes the column names, data types, and any nested structures.
     - This schema information allows for schema evolution, enabling changes to the schema over time without breaking existing queries.
  
  3. **Row Groups**:
     - A Parquet file is divided into row groups. Each row group contains a large number of rows, typically on the order of millions.
     - Row groups allow for efficient scanning and retrieval of data because they can be processed independently.
  
  4. **Column Chunks**:
     - Within each row group, data is stored in column chunks.
     - Column chunks store the actual data for each column and are compressed individually, which improves both compression efficiency and query performance.
  
  5. **Page Metadata**:
     - Each column chunk is further divided into pages.
     - Page metadata includes information such as the number of values, the maximum and minimum values (for efficient filtering), and compression information.
  
  6. **Statistics**:
     - Parquet files store statistics at multiple levels (file, row group, and page) to help with query optimization.
     - These statistics include min/max values, null counts, and distinct counts.
     - They enable Parquet readers to skip reading parts of the file that do not match query predicates, significantly improving performance.


### 3. CSV
- **Description**: CSV (Comma-Separated Values) is a simple file format used to store tabular data, such as a spreadsheet or database.
- **Usage Example**:
  ```python
  # Writing DataFrame to CSV format
  df.write.format("csv").option("header", "true").save("/mnt/devmadeadlssa/csv")

  # Reading DataFrame from CSV format
  csv_df = spark.read.format("csv").option("header", "true").load("/mnt/devmadeadlssa/csv")
  ```

### 4. JSON
- **Description**: JSON (JavaScript Object Notation) is a lightweight data interchange format that's easy for humans to read and write, and easy for machines to parse and generate.
- **Usage Example**:
  ```python
  # Writing DataFrame to JSON format
  df.write.format("json").save("/mnt/devmadeadlssa/json")

  # Reading DataFrame from JSON format
  json_df = spark.read.format("json").load("/mnt/devmadeadlssa/json")
  ```

### 5. Avro
- **Description**: Avro is a row-oriented remote procedure call and data serialization framework developed within Apache's Hadoop project. It uses JSON for defining data types and protocols, and serializes data in a compact binary format.
- **Usage Example**:
  ```python
  # Writing DataFrame to Avro format
  df.write.format("avro").save("/mnt/devmadeadlssa/avro")

  # Reading DataFrame from Avro format
  avro_df = spark.read.format("avro").load("/mnt/devmadeadlssa/avro")
  ```

### 6. ORC
- **Description**: ORC (Optimized Row Columnar) is a columnar storage file format that is highly optimized for big data workloads. It provides high compression and the ability to handle complex data types.
- **Usage Example**:
  ```python
  # Writing DataFrame to ORC format
  df.write.format("orc").save("/mnt/devmadeadlssa/orc")

  # Reading DataFrame from ORC format
  orc_df = spark.read.format("orc").load("/mnt/devmadeadlssa/orc")
  ```

![image](https://github.com/user-attachments/assets/5502dfeb-a03d-4755-8e28-412cf06f3e40)


# Infering schema vs defining explicitly

Inferring a schema and explicitly defining a schema in PySpark each have their pros and cons. However, in many cases, explicitly defining the schema (such as using a JSON schema with `StructType`) is preferred over schema inference. Here’s why:

### Inferring Schema

When you use PySpark to infer the schema of a DataFrame, it automatically determines the data types of columns based on the data. This can be done using:

```python
df = spark.read.option("inferSchema", "true").csv("path/to/csvfile.csv")
```

**Pros of Inferring Schema:**
- **Convenience**: Quick and easy, especially for small or simple datasets.
- **Automatic**: No need to manually specify the data types, which can save time for exploratory data analysis.

**Cons of Inferring Schema:**
- **Performance**: Schema inference requires reading a significant portion of the data, which can be slow for large datasets.
- **Inaccuracy**: PySpark might infer incorrect data types if the sample data is not representative of the entire dataset. For example, if a column has integers but later contains strings, it might misinterpret the column type.
- **Consistency**: If the dataset schema changes (e.g., new columns are added, or data types change), the inferred schema might not be consistent across different reads.

### Defining Schema Explicitly

When you explicitly define the schema, you manually specify the data types and structure of the DataFrame. This can be done using `StructType` and `StructField`:

```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("Name", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Salary", IntegerType(), True)
])

df = spark.read.schema(schema).csv("path/to/csvfile.csv")
```

Alternatively, you can define the schema in JSON and load it as shown in your example:

```python
import json
from pyspark.sql.types import StructType

# Define the schema in JSON
schema_json = dbutils.fs.head("/tmp7/myschema.json", 1024)
knownSchema = StructType.fromJson(json.loads(schema_json))

# Use the schema to read the data
df = spark.read.schema(knownSchema).csv("path/to/csvfile.csv")
```

**Pros of Defining Schema Explicitly:**
- **Performance**: No need to scan the data to infer the schema, which can significantly speed up the read process for large datasets.
- **Accuracy**: Ensures that the data types are correctly interpreted as intended, avoiding potential misinterpretations by the schema inference process.
- **Consistency**: Provides a consistent schema regardless of changes in the underlying data, which is crucial for production applications.
- **Data Quality**: Helps in validating the data types and structure, which can catch data quality issues early in the ETL process.

**Cons of Defining Schema Explicitly:**
- **Manual Effort**: Requires manual specification of the schema, which can be time-consuming, especially for datasets with many columns.
- **Maintenance**: If the dataset schema changes frequently, the manually defined schema needs to be updated accordingly.

![image](https://github.com/user-attachments/assets/21af6bd4-f280-487a-8259-cf9590dcc3e1)
 - Here it took 41 seconds

![image](https://github.com/user-attachments/assets/ecac13fb-f3e3-4c8c-8d0e-8fff1b949d9b)
 - It did the work less than a second



## Cheat sheet Comparison
| Type    | <span style="white-space:nowrap">Inference Type</span> | <span style="white-space:nowrap">Inference Speed</span> | Reason                                          | <span style="white-space:nowrap">Should Supply Schema?</span> |
|---------|--------------------------------------------------------|---------------------------------------------------------|----------------------------------------------------|:--------------:|
| <b>CSV</b>     | <span style="white-space:nowrap">Full-Data-Read</span> | <span style="white-space:nowrap">Slow</span>            | <span style="white-space:nowrap">File size</span>  | Yes            |
| <b>Parquet</b> | <span style="white-space:nowrap">Metadata-Read</span>  | <span style="white-space:nowrap">Fast/Medium</span>     | <span style="white-space:nowrap">Number of Partitions</span> | No (most cases)             |
| <b>Tables</b>  | <span style="white-space:nowrap">n/a</span>            | <span style="white-space:nowrap">n/a</span>            | <span style="white-space:nowrap">Predefined</span> | n/a            |
| <b>JSON</b>    | <span style="white-space:nowrap">Full-Read-Data</span> | <span style="white-space:nowrap">Slow</span>            | <span style="white-space:nowrap">File size</span>  | Yes            |
| <b>Text</b>    | <span style="white-space:nowrap">Dictated</span>       | <span style="white-space:nowrap">Zero</span>            | <span style="white-space:nowrap">Only 1 Column</span>   | Never          |
| <b>JDBC</b>    | <span style="white-space:nowrap">DB-Read</span>        | <span style="white-space:nowrap">Fast</span>            | <span style="white-space:nowrap">DB Schema</span>  | No             |

-------------------------------------------------------

# Ashok's notes

##Reading CSV
- `spark.read.csv(..)`
- There are a large number of options when reading CSV files including headers, column separator, escaping, etc.
- We can allow Spark to infer the schema at the cost of first reading in the entire file.
- Large CSV files should always have a schema pre-defined.

## Reading Parquet
- `spark.read.parquet(..)`
- Parquet files are the preferred file format for big-data.
- It is a columnar file format.
- It is a splittable file format.
- It offers a lot of performance benefits over other formats including predicate pushdown.
- Unlike CSV, the schema is read in, not inferred.
- Reading the schema from Parquet's metadata can be extremely efficient.

## Reading Tables
- `spark.read.table(..)`
- The Databricks platform allows us to register a huge variety of data sources as tables via the Databricks UI.
- Any `DataFrame` (from CSV, Parquet, whatever) can be registered as a temporary view.
- Tables/Views can be loaded via the `DataFrameReader` to produce a `DataFrame`
- Tables/Views can be used directly in SQL statements.

## Reading JSON
- `spark.read.json(..)`
- JSON represents complex data types unlike CSV's flat format.
- Has many of the same limitations as CSV (needing to read the entire file to infer the schema)
- Like CSV has a lot of options allowing control on date formats, escaping, single vs. multiline JSON, etc.

## Reading Text
- `spark.read.text(..)`
- Reads one line of text as a single column named `value`.
- Is the basis for more complex file formats such as fixed-width text files.

## Reading JDBC
- `spark.read.jdbc(..)`
- Requires one database connection per partition.
- Has the potential to overwhelm the database.
- Requires specification of a stride to properly balance partitions.

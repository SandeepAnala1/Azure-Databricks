
# Incremental Loading in Batch processing

Incremental loading is a technique used in data integration and data warehousing to update the target data store with only the changes (new, updated, or deleted records) that have occurred since the last data load. This method is efficient as it reduces the amount of data processed and transferred, making the ETL (Extract, Transform, Load) process faster and more resource-efficient.

## Incremental Loading in Azure Data Factory (ADF)

Azure Data Factory (ADF) supports incremental loading using various methods such as watermarking, change data capture (CDC), and incremental file loading. Here’s how you can perform incremental loading using watermarking in ADF:

1. **Create a Watermark Table**: Maintain a watermark table to keep track of the last load timestamp.
   - This table will store the `LastModifiedDate`, which indicates the last time data was loaded.

2. **Retrieve the Last Watermark Value**: 
   - Fetch the last loaded timestamp from the watermark table. This will be used to load only the data that has changed since this timestamp.

3. **Set the Current Watermark Value**:
   - Create a variable (e.g., `v_system_date`) to store the current system date, which will be used as the new watermark value for this load.

4. **Filter the Source Data**:
   - Use the retrieved watermark value to filter the source data. Load only the records that have a `LastModifiedDate` greater than the retrieved watermark value.

5. **Write Data to Bronze Table**:
   - Load the filtered data into a staging or bronze table. This table holds the raw data in its initial form.

6. **Transform Data**:
   - Apply any necessary transformations to the data in the bronze table.

7. **Load Data to Gold Table**:
   - Merge the transformed data into the target gold table. Use UPSERT (Update if matched, Insert if not matched) logic to ensure that existing records are updated, and new records are inserted.

8. **Update Watermark Table**:
   - After successfully loading the data, update the watermark table with the current system date (`v_system_date`), so it can be used as the watermark for the next incremental load.

## Incremental Loading in Databricks

Databricks also supports incremental loading, leveraging Delta Lake for efficient data processing. The steps are similar but may involve different implementations.

1. **Create a Watermark Table**: 
   - Create a Delta table to store the `LastModifiedDate`.

2. **Retrieve the Last Watermark Value**:
   - Query the watermark table to get the last loaded timestamp.

3. **Set the Current Watermark Value**:
   - Create a variable (e.g., `v_system_date`) to store the current system date.

4. **Filter the Source Data**:
   - Use Spark’s DataFrame API to read the source data, filtering it based on the retrieved watermark value using the `modifiedAfter` option.

5. **Write Data to Bronze Table**:
   - Write the filtered data to a bronze Delta table, which holds the raw data.

6. **Read Data from Bronze Table**:
   - Read the data from the bronze table for further processing.

7. **Transform Data**:
   - Apply transformations to the data read from the bronze table.

8. **Load Data to Gold Table**:
   - Use the Delta Lake `merge` operation to merge the transformed data into the gold table, ensuring that existing records are updated and new records are inserted.

9. **Update Watermark Table**:
   - After successful loading, update the watermark table with the current system date (`v_system_date`).

## Step-by-Step Process (from the Screenshot)
![image](https://github.com/user-attachments/assets/cecf2ddd-2f7c-4016-9556-6573bc9b2bd3)

1. **Folder Structure**:
   - The folder `Invoices` contains multiple invoice files (`Invoice1`, `Invoice2`, `Invoice3`).

2. **Ingest Latest File**:
   - Read the latest file for processing.

3. **Create Watermark Table**:
   - `LastModiedDate` table to store the watermark.

4. **Past Value**:
   - Store the current value in the watermark table.

5. **Get Old Watermark Value**:
   - Retrieve the previous watermark value from the table.

6. **Set Current System Date**:
   - Create a variable `v_system_date` to store the current date.

7. **Read File with Filter**:
   - Use `spark.read` to read the file, filtering data modified after the old watermark value.

8. **Create Bronze Table**:
   - Store the filtered raw data into the bronze table.

9. **Read Data from Bronze Table**:
   - Read the data from the bronze table for further processing.

10. **Write Transformations**:
    - Apply necessary transformations to the data.

11. **Load Data to Gold Table**:
    - Merge (UPSERT) the transformed data into the gold table.

12. **Update Watermark**:
    - Update the watermark table with the current system date (`v_system_date`).

----------------------------------------------

# Minor setbacks in Incremental Loading
Incremental loading is a powerful and efficient technique for data integration, but it does have some drawbacks. Here are some common challenges and limitations:

1. **Complexity in Change Detection**:
   - Identifying and capturing only the changed data can be complex, especially when dealing with heterogeneous data sources.
   - Handling different types of changes (inserts, updates, deletes) correctly requires careful design.

2. **Data Consistency**:
   - Ensuring data consistency across the source and target systems can be challenging.
   - Partial failures in the ETL process can lead to inconsistencies if not handled properly.

3. **Handling Schema Changes**:
   - Changes in the source data schema (e.g., adding or dropping columns) can complicate the incremental loading process.
   - The ETL process needs to be robust enough to adapt to these changes.

4. **Latency**:
   - Incremental loading typically operates in batch mode, which introduces some latency.
   - Real-time data processing requirements may not be met.

5. **Resource Management**:
   - Managing resources efficiently during incremental loading can be challenging, especially when dealing with large volumes of data.
   - Proper indexing and partitioning strategies are required to optimize performance.

6. **Data Duplication**:
   - Incorrect handling of the watermark values can lead to data duplication or data loss.
   - Careful management of watermark updates is necessary to avoid these issues.

-------------------------------------------------------------

## Scripting

1. **Base Data Directory**:
   ```python
   base_data_dir="/mnt/devmadeadlssa/landing"
   ```

   This variable defines the base directory where the data files are stored.

2. **Print Statements and Functions**:
   ```python
   print('Ashok')

   name='My Name is Ashok'

   def getName():
       name='My Name is Ashok Kumar'
       return (name)

   myname=getName()

   print(myname)
   ```

   These lines are simple print statements and functions to demonstrate basic Python functionality. They don't contribute to the incremental loading process.

3. **Schema Definition**:
   ```python
   def getSchema():
       return """InvoiceNumber string, CreatedTime bigint, StoreID string, PosID string, CashierID string,
                CustomerType string, CustomerCardNo string, TotalAmount double, NumberOfItems bigint, 
                PaymentMethod string, TaxableAmount double, CGST double, SGST double, CESS double, 
                DeliveryType string,
                DeliveryAddress struct<AddressLine string, City string, ContactNumber string, PinCode string, 
                State string>,
                InvoiceLineItems array<struct<ItemCode string, ItemDescription string, 
                    ItemPrice double, ItemQty bigint, TotalValue double>>
            """
   ```

   This function defines the schema of the invoice data. It describes the structure of the data that will be read from the source files.

4. **Read Invoices Function**:
   ```python
   def readInvoices():
       from pyspark.sql.functions import input_file_name
       return (
           spark.read.format("json")
           .schema(getSchema())
           .load(f"{base_data_dir}/streaming/invoices")
           .withColumn("InputFile", input_file_name())
       )
   ```

   This function reads invoice data from JSON files located in the specified directory. It uses the schema defined in the previous function and adds a column to capture the input file name.

5. **Process Function**:
   ```python
   def process():
       print(f"\nStarting Bronze Stream...", end="")
       invoicesDF = readInvoices()
       invoicesDF.write.mode("append").saveAsTable("invoices_bz1")
       print("Done")
   ```

   This function initiates the reading of invoices and writes the data to a bronze table (`invoices_bz1`). The mode is set to "append," indicating that new data will be appended to the existing table.

## Implementing Incremental Loading

To implement incremental loading effectively, we need to ensure that we only load new or changed data since the last load. The following steps outline a more complete approach using watermarking or change data capture (CDC) concepts:

1. **Create Watermark Table**:
   - Maintain a table to track the `LastModifiedDate`.

   ```python
   # Assuming a Delta table is used for the watermark
   spark.sql("""
   CREATE TABLE IF NOT EXISTS watermark_table (
       table_name STRING,
       last_modified_date TIMESTAMP
   )
   """)
   ```

2. **Retrieve the Last Watermark Value**:
   - Fetch the last loaded timestamp from the watermark table.

   ```python
   last_modified_date = spark.sql("""
   SELECT last_modified_date
   FROM watermark_table
   WHERE table_name = 'invoices'
   """).collect()[0]['last_modified_date']
   ```

3. **Read Invoices with Filter**:
   - Use the retrieved watermark value to filter the source data.

   ```python
   from pyspark.sql.functions import col

   def readInvoices():
       from pyspark.sql.functions import input_file_name
       return (
           spark.read.format("json")
           .schema(getSchema())
           .load(f"{base_data_dir}/streaming/invoices")
           .withColumn("InputFile", input_file_name())
           .filter(col("CreatedTime") > last_modified_date)
       )
   ```

4. **Process Function with Watermark Update**:
   - Update the `process` function to include watermark handling.

   ```python
   def process():
       print(f"\nStarting Bronze Stream...", end="")
       invoicesDF = readInvoices()
       invoicesDF.write.mode("append").saveAsTable("invoices_bz1")

       # Update the watermark table with the current timestamp
       current_time = spark.sql("SELECT MAX(CreatedTime) as current_time FROM invoices_bz1").collect()[0]['current_time']
       spark.sql(f"""
       UPDATE watermark_table
       SET last_modified_date = '{current_time}'
       WHERE table_name = 'invoices'
       """)
       
       print("Done")
   ```

5. **Run the Process**:
   - Execute the process function to perform the incremental load.

   ```python
   process()
   ```

By implementing these steps, you ensure that only new or modified records are loaded into the target table, thereby achieving efficient incremental loading.

------------------------------------------------------------------------------------------------------

# Structured Streaming in Spark

**Structured Streaming** is a scalable and fault-tolerant stream processing engine built on the Spark SQL engine. It enables developers to work with real-time data streams using the same APIs that are used for batch processing in Spark. This approach overcomes many drawbacks of batch processing incremental loads by providing continuous processing capabilities, which helps in low-latency and real-time data processing.

## Structured Streaming Architecture

![image](https://github.com/user-attachments/assets/fbfa2665-d3d0-46de-8e59-207eb2b7bfaf)

1. **Streaming Source**: The process begins with a streaming source, which continuously produces data. This could be a directory (`.../data`), a Kafka topic, or another data stream source.

2. **Streaming Query**: This component represents the query defined by the user to process the streaming data. The query specifies how the data should be read, transformed, and written.

3. **Trigger Micro Batch**: Structured streaming operates in micro-batches, where it periodically triggers to process the new data that has arrived since the last trigger. Each trigger initiates a micro-batch.

4. **Micro-Batch Input Data**: In each trigger, a micro-batch of data (`file1` in this case) is taken from the streaming source for processing.

5. **Execution Plan**: The execution plan defines the operations that need to be performed on each micro-batch of data. This includes reading the data, applying transformations, and writing the results.

6. **ReadStream**: This step reads the micro-batch of data (`file1`) from the streaming source.

7. **Transform**: After reading the data, the next step is to apply the specified transformations. These transformations could include filtering, aggregation, joining with other data, etc.

8. **WriteStream**: After the transformations are applied, the resulting data is written to the designated sink, which could be a file system, a database, or another storage system.

9. **Checkpoint**: To ensure fault tolerance and recoverability, structured streaming maintains checkpoints. These checkpoints keep track of the progress of the streaming query so that, in case of a failure, the system can resume processing from the last checkpointed state.

10. **Feedback Loop**: The dotted lines indicate the feedback loop from the checkpoint to the streaming query, ensuring that the state is managed and preserved across micro-batches.

Overall, structured streaming breaks down the continuous stream of data into manageable micro-batches, applies the user-defined transformations, and ensures fault tolerance through checkpoints. This architecture provides a powerful and flexible framework for real-time data processing.

## Key Features:
- **Continuous Processing**: Processes data in real-time as it arrives.
- **Fault Tolerance**: Automatically recovers from failures.
- **Scalability**: Scales horizontally to handle large data volumes.
- **Unified Batch and Stream Processing**: Uses the same API for batch and stream processing.
- **Exactly-once Semantics**: Ensures that each record is processed exactly once.

### Reading Data from ADLS in Structured Streaming

To read data from Azure Data Lake Storage (ADLS), you need to mount the ADLS storage to Databricks. Here is the mounting code and the steps to read data from invoice text files (invoice1, invoice2, invoice3) located in ADLS.

## Mounting ADLS to Databricks

```python
# Set up the configuration for the ADLS mount
configs = {
  "fs.azure.account.auth.type": "OAuth",
  "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id": "<application-id>",
  "fs.azure.account.oauth2.client.secret": "<application-secret>",
  "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<directory-id>/oauth2/token"
}

# Mount the ADLS storage
dbutils.fs.mount(
  source = "abfss://<container-name>@<account-name>.dfs.core.windows.net/",
  mount_point = "/mnt/adls",
  extra_configs = configs
)
```

Replace `<application-id>`, `<application-secret>`, `<directory-id>`, `<container-name>`, and `<account-name>` with your ADLS and Azure AD details.

## Reading Data from Mounted ADLS

```python
# Define the schema for the invoice files
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
    StructField("InvoiceID", StringType(), True),
    StructField("CustomerID", StringType(), True),
    StructField("InvoiceDate", StringType(), True),
    StructField("TotalAmount", IntegerType(), True)
])

# Read data from the invoice text files
input_path = "/mnt/adls/invoices/"
invoice_stream = (
    spark.readStream
    .option("header", "true")
    .schema(schema)
    .csv(input_path)
)
```

## Performing Aggregations

```python
from pyspark.sql.functions import window, sum

# Aggregate the total amount by customer and windowed by 1 hour
aggregated_stream = (
    invoice_stream
    .withWatermark("InvoiceDate", "1 hour")
    .groupBy(
        window(invoice_stream.InvoiceDate, "1 hour"),
        invoice_stream.CustomerID
    )
    .agg(sum("TotalAmount").alias("TotalAmount"))
)
```

## Writing the Aggregated Data

You can write the aggregated stream to various sinks like console, file system, or databases.

#### Writing to Console

```python
query = (
    aggregated_stream
    .writeStream
    .outputMode("update")
    .format("console")
    .start()
)
query.awaitTermination()
```

#### Writing to a Parquet File

```python
output_path = "/mnt/adls/output/aggregated_invoices/"
query = (
    aggregated_stream
    .writeStream
    .outputMode("update")
    .format("parquet")
    .option("checkpointLocation", "/mnt/adls/checkpoints/aggregated_invoices")
    .start(output_path)
)
query.awaitTermination()
```

#### Writing to a Delta Table

```python
output_path = "/mnt/adls/delta/aggregated_invoices/"
query = (
    aggregated_stream
    .writeStream
    .outputMode("update")
    .format("delta")
    .option("checkpointLocation", "/mnt/adls/checkpoints/aggregated_invoices")
    .start(output_path)
)
query.awaitTermination()
```

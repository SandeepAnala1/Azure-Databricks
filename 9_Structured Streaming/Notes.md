
# Incremental Loading

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


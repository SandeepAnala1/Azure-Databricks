# Index

- [Interesting Fact](#interesting-fact)
- [Which one is better and why?](#which-one-is-better-and-why)
- [Secret Scopes](#secret-scopes)
- [Pyspark](#pyspark)
  - [File Reading Options](#file-reading-options)
  - [Resilient Distributed Dataset (RDD) in PySpark](#resilient-distributed-dataset-rdd-in-pyspark)
  - [Job, Stage, Task](#job-stage-task)
  - [StructType](#structtype)
  - [StructField](#structfield)
  - [PERMISSIVE Mode](#1-permissive)
  - [FAILFAST Mode](#2-failfast)
  - [DROPMALFORMED Mode](#3-dropmalformed)
  - [Summary](#summary)
  - [Find the handson here](#find-the-handson-here)

### Interesting Fact
- Even if you are working on Azure Databricks, you can access the storage account of aws through mounting, because it just needs Client ID, Tenant ID, Credentials(secret)[Service Principle], container name & storage account name.

**Service Principal**: An identity created for use with applications, hosted services, and automated tools to access Azure resources.

**Managed Identity**: An Azure AD identity managed by Azure, automatically created and maintained for use with Azure services to access other Azure resources without needing explicit credentials.

**Difference**: Service Principals require manual management and credential handling, while Managed Identities are automatically managed by Azure and eliminate the need for credential management.

### which one is better and why?

**Managed Identity** is generally better for most use cases in **Azure** because:

1. **Automatic Management**: Azure handles the creation, management, and rotation of credentials, reducing the risk of human error and enhancing security.
2. **Simplified Configuration**: Managed Identities simplify the configuration process by eliminating the need for manual credential storage and management.
3. **Enhanced Security**: By not requiring explicit credentials, Managed Identities reduce the risk of credential leakage or misuse.
4. **Seamless Integration**: Managed Identities are designed to seamlessly integrate with other Azure services, making them ideal for a wide range of scenarios within the Azure ecosystem.

- In contrast, **Service Principals** require more manual management of credentials and configurations, making them more complex and potentially less secure if not handled properly.
-------------------------------

Outside of Azure, **Service Principals** are generally better because:

1. **Versatility**: Service Principals can be used across various environments and platforms, not limited to Azure services.
2. **Customizability**: They provide more flexibility for complex and custom configurations that might be necessary in diverse or multi-cloud environments.
3. **Cross-Platform Access**: Service Principals can authenticate to Azure resources from applications running outside Azure, such as on-premises environments or other cloud platforms.

**Managed Identities** are limited to Azure services and cannot be used directly outside the Azure environment, making Service Principals the more suitable choice for scenarios that extend beyond Azure.

--------------------------------

# Last time we hardcoded ids & keys for mounting, let's learn how can we save it in vault

## Azure Key Vault
**Key**: A cryptographic key used for encryption, decryption, or signing operations stored in Azure Key Vault.

**Secret**: Any confidential information such as passwords, connection strings, or API keys stored securely in Azure Key Vault.

**Certificate**: An SSL/TLS certificate along with its private key stored and managed in Azure Key Vault for secure communications.

- Make sure you select Vault Access Policy while creating it
![image](https://github.com/SandeepAnala1/Azure-Databricks/assets/163712602/9ca4f09d-bc96-4303-bf64-a869cbf792c0)

- Once we create secret we can get only versions
![image](https://github.com/SandeepAnala1/Azure-Databricks/assets/163712602/b4e06b52-6609-4e12-b84d-6f29009d4501)

- Now how to access this through Secret Scopes?
-----------------------------------------------

# Secret Scopes
1) Key Vault Backed Secret Scopes: Secretts are stored in Key Vault, but scope is created to access it from Databricks
3) Databricks Backed Secret Scopes: Secrets are also stored in Databricks and accessed in databricks

`https://adb-4473727248771931.11.azuredatabricks.net/#secrets/createScope` 
![image](https://github.com/SandeepAnala1/Azure-Databricks/assets/163712602/b3ba1ba6-6b33-4bc8-8581-fdf62e1dc7fc)

- DNS Name = Vault URI

![image](https://github.com/SandeepAnala1/Azure-Databricks/assets/163712602/73111900-8eb4-4a95-96f5-abfe495327ad)

![image](https://github.com/SandeepAnala1/Azure-Databricks/assets/163712602/145c377b-bea4-4680-a030-9b81100c5158)

- See here, it's not even showing the credentials, it says redacted. It is for security purposes

![image](https://github.com/SandeepAnala1/Azure-Databricks/assets/163712602/0aa92f02-354e-4197-90ca-7bbfe15b3c7b)

------------------------------------------------------------------------------------------------

# Pyspark

## File Reading Options

# Resilient Distributed Dataset (RDD) in PySpark

- **Concept (PySpark)**:
  - RDDs are fundamental data structures in Apache Spark, representing immutable, distributed collections of objects processed in parallel across a cluster.

- **Application Submission Process**:
  - When you submit an application to Spark, the following steps occur:

1. **Application Master Container Allocation**:
   - The Application Master Container is allocated to manage the application lifecycle.

2. **Launching the Main Method (PySpark Driver)**:
   - The main method for the application is launched within this container.

3. **Python Code Conversion**:
   - PySpark converts Python code into JVM bytecode using the Py4j library, as the core logic of Spark is implemented in Java.

4. **Application Driver**:
   - The JVM launched by the driver is referred to as the Application Driver.
   - The Application Driver divides the code and data, requesting resources from the Cluster Manager.

5. **Executor Containers and JVMs**:
   - Executor containers are then launched, each with its own JVM instance.
   - The Application Driver distributes data to these JVMs.

6. **RDD Data Type**:
   - The distributed data is represented as RDDs, hence the term Resilient Distributed Dataset.

RDDs are the distributed data structures that enable parallel processing in Spark, and PySpark leverages JVM through Py4j for execution. The process involves distributing both code and data across executor JVMs managed by the Application Driver.

------------------------------------------

# Job, Stage, Task

- Job: Any Action --> Any code which brings the data back to driver is called an action
- Stage: Any Wide Transformation -- These run in Sequential
- Task: Any Narrow Transformation -- These run in parallel

-----------------------------------------

  - Here it is reading the file & loading it into the memory
  ![image](https://github.com/SandeepAnala1/Azure-Databricks/assets/163712602/18346b93-501c-40af-b65f-2a86b9f1696e)
  
  - Dataframes are the logical table inside the memory gets created as soon as we read the file
  
  ![image](https://github.com/SandeepAnala1/Azure-Databricks/assets/163712602/281fbd4b-a5db-41a7-9f2b-55e3ab3e922e)
  
  - The partitioned data are send to worker nodes
  
  ![image](https://github.com/SandeepAnala1/Azure-Databricks/assets/163712602/d045e3a8-9793-41f0-a4fe-42ed254766b1)
  
  - We should avoid using Inferschema as it creates other job and it will take time
  
  ![image](https://github.com/SandeepAnala1/Azure-Databricks/assets/163712602/a87b3220-1eff-46fc-bb9d-545ab637cc72)
  
  - The reason, it is not displaying any output is because, the data is still present in the worker node, to bring it back to driver node we need to perform action
  
  - In PySpark, transformations like `map` are lazily evaluated, meaning that they do not immediately execute when called. Instead, they create a lineage of operations to be performed when an action is triggered. The action in your code should be something like `collect`, `count`, `show` or `take`, which forces the computation to be executed.
  
  ![image](https://github.com/SandeepAnala1/Azure-Databricks/assets/163712602/3e3b7ef1-684d-41a2-9d68-578c5014d764)
  
  In PySpark, `StructType` and `StructField` are used to define the schema for DataFrames. They allow you to specify the structure of your data, including column names, data types, and whether the columns can contain null values.
  
  ### StructType
  
  **Definition**: `StructType` is a collection of `StructField` objects that defines the schema of a DataFrame. It is essentially a list of fields, where each field represents a column.
  
  ### StructField
  
  **Definition**: `StructField` represents a single column in a DataFrame. It includes:
  
  ![image](https://github.com/SandeepAnala1/Azure-Databricks/assets/163712602/2b6a82d6-a40b-4883-9feb-9c52a3a9d330)

When reading files into a DataFrame in PySpark, you can specify how Spark should handle malformed data using the `mode` option. The three common modes are `PERMISSIVE`, `FAILFAST`, and `DROPMALFORMED`.

  ## 1. PERMISSIVE
  
  **Definition**: The `PERMISSIVE` mode is the default mode. In this mode, Spark tries to parse all the records, even if some of them are malformed.
  
  **Behavior**:
  - When a row is malformed, Spark sets all the columns to `null` and puts the malformed data into a separate field called `_corrupt_record`.
  - This mode ensures that all data is read and included in the DataFrame, even if some rows are corrupted.
  
  **Example**:
  ```python
  df = spark.read.option("mode", "PERMISSIVE").csv("path/to/file.csv")
  ```
  
  ## 2. FAILFAST
  
  **Definition**: The `FAILFAST` mode stops processing as soon as it encounters any malformed row.
  
  **Behavior**:
  - If Spark encounters a malformed row, it throws an exception and stops reading the file.
  - This mode is useful when you want to ensure data quality and do not want to proceed with corrupted data.
  
  **Example**:
  ```python
  df = spark.read.option("mode", "FAILFAST").csv("path/to/file.csv")
  ```
  
  ## 3. DROPMALFORMED
  
  **Definition**: The `DROPMALFORMED` mode drops any rows that contain malformed data.
  
  **Behavior**:
  - Any row that does not conform to the schema is ignored and not included in the DataFrame.
  - This mode helps in keeping only the well-formed data, but it might lead to data loss if there are many malformed rows.
  
  **Example**:
  ```python
  df = spark.read.option("mode", "DROPMALFORMED").csv("path/to/file.csv")
  ```
  ### Summary
  
  - **PERMISSIVE**: Tries to read all rows, marking malformed ones with `null` and placing malformed data in `_corrupt_record`.
  - **FAILFAST**: Stops reading and throws an error when a malformed row is encountered.
  - **DROPMALFORMED**: Ignores and drops malformed rows, keeping only well-formed data in the DataFrame.



Find the handson [here](https://github.com/SandeepAnala1/Azure-Databricks/blob/main/3_SecretScopes%2BFile%20Reading%20Options/Files/1.File%20Reading%20Options.py)






























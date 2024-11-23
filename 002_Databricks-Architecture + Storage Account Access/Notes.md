### If you gone through spark internals, you can find so much information and calculations on memories in spark submit. During 2014, people used to submit their jobs by spark submit. So, Databricks came with the managed service where all the struggle of spark submit i.e., calculating the executor memory, driver memory for each job is taken care by Databricks itself.

# Index

- [Why Databricks?](#why-databricks)
- [Azure Databricks Architecture](#azure-databricks-architecture)
- [Compute: Cluster](#compute-cluster)
- [Hand's ON](#hands-on)
  - [1) Accessing it through Access key](#1-accessing-it-through-access-key)
  - [2) SAS Key](#2-sas-key)
  - [3) Service Principle](#3-service-principle)
  - [4) IAM](#4-iam)
  - [5) Mounting](#5-mounting)


# Why Databricks?
﻿
- Databricks is a cloud-based platform for big data analytics that is often used with Apache Spark, which is a popular open-source, distributed data processing framework. While Databricks incorporates Apache Spark, it provides several benefits and additional features that can make it more attractive for certain use cases and organizations.

- Here are some of the benefits of using Databricks over Apache Spark alone:
  - Ease of Setup and Management: Databricks simplifies the setup and management of Spark clusters.
  - It abstracts away many of the low-level infrastructure tasks, making it easier for data engineers and data scientists to focus on their data and analytics work without worrying about cluster provisioning and tuning.

- ﻿**Unified Environment:** Databricks provides a unified workspace for data engineering, data science, and machine learning. It combines Spark with notebooks, data visualization, and collaborative tools in a single platform, making it easier to work with big data in an integrated manner.
- **Collaboration and Sharing:** Databricks supports collaboration features, allowing multiple users to work on the same notebook and share results with colleagues. This makes it suitable for teams working on big data projects.
- **Scalability:** Databricks can automatically scale clusters up or down based on the workloads. This elasticity can help optimize costs and ensure resources are
allocated efficiently.
- **Managed Services:** Databricks is a managed service offered on various cloud providers (e.g., AWS, Azure, Google Cloud), which means that users don't need to worry about infrastructure management, security, or upgrades. Databricks takes care of these aspects.
- **Optimized Performance:** Databricks offers optimization and fine-tuning for Spark, improving performance and resource management. It provides Databricks Runtime, which includes performance enhancements and optimizations that are not available in open-source Spark.
- **Integration with Other Tools:** Databricks integrates with various data
engineering and data science tools, making it easier to work with an ecosystem of related services and libraries.
- **Security and Compliance:** Databricks offers enterprise-level security features and supports compliance standards, which can be crucial for organizations that need to maintain data privacy and security.

-------------------------------------------------------------------------

# Azure Databricks Architecture

- **Managed Service:** Infrastructure is managed by databricks
- **Databricks doesn't store any of your data, it will be in your customer subscription**

![image](https://github.com/SandeepAnala1/Azure-Databricks/assets/163712602/6bbc93b7-54fb-4a27-a189-260c241cf205)

- Once you create a Databricks service in Azure portal, it creates a blob storage, VMs & Virtual Network
- And Vnet is providing additional security, so that no one else can access from outside. Just like VPN allows certain range of IPs
  
![image](https://github.com/SandeepAnala1/Azure-Databricks/assets/163712602/5365662a-d847-4b6d-9e95-c64e5d8255b8)

-  Azure AD's: Whenever you created a new subscription, there is an entra ID created. Databricks does the authentication using Azure AD

---------------------------------------------

# Compute: Cluster

![image](https://github.com/SandeepAnala1/Azure-Databricks/assets/163712602/db89c1e5-65b6-4f0b-8591-1cf496e740b9)

### All purpose compute vs Job compute

![image](https://github.com/SandeepAnala1/Azure-Databricks/assets/163712602/9ebe62bc-96ee-468b-8b06-417d66233fba)

![image](https://github.com/SandeepAnala1/Azure-Databricks/assets/163712602/6509806d-cd55-4dc1-a250-1996814fe532)

### Multi Node vs Single Node
- More Virtual Machines & single Virtual Machine
- In Multi node, you'll be having both Driver & Worker nodes, but in single node you can find one

### Databricks Runtime
- It has all the libraries to run your spark cluster & any additional performance tuning + any enhancement done for data bricks

### Worker type
- It has Executor memory, cores

> Interview Question 1: Generally for Big Data Processing, 11-25 worker nodes are used with 128 GB RAM each + 4 cores. Question may be asked like what is the cluster size you have used
> Interview Question 2: What kind of clusters are in your project? All purpose cluster | Job cluster

![image](https://github.com/SandeepAnala1/Azure-Databricks/assets/163712602/7b574685-5ddb-421b-a607-5e251effbd28)

- If this is not checked, our cluster will keep on running and it will consume our units.
- Once you click on create compute, Virtual Machines start getting created for you & Databricks Cluster Manager will allocate driver and worker nodes. So it requests the Azure Resurce Manager for the required VMS & it also access the blob storage

- **Hive Metastore**: It is a catalog to store the metadata of a table or database
    > Metadata: Information about your data Eg: data types, schema etc

### How do you access your storage account outside Azure??
1) Service Principle - Only Authentication
2) Access Key  -- Both Authentication & Authorization -- By default it gives you admin access
3) SAS key -- Both Authentication & Authorization -- It limits the access
4) Managed Identity

- **Authentication**: The process of verifying the identity of a user or system. Simply it's validating your credentials.
- **Authorization**: The process of determining the permissions or access levels granted to a user or system.

# Hand's ON
- Created ADLS Gen 2 Storage account with 3 containers raw, processed & presentation
  
![image](https://github.com/SandeepAnala1/Azure-Databricks/assets/163712602/6649d115-8fa0-4e6e-9a6e-80662d0dcfe7)

## 1) Accessing it through Access key
  - Security + Networking -> Access Key
  ![image](https://github.com/SandeepAnala1/Azure-Databricks/assets/163712602/8797e7f0-477e-4893-b547-99fba573149b)

  `dbutils` is a utility library provided by Databricks to facilitate various common tasks when working within a Databricks notebook environment. It provides a range of functionalities to simplify workflows, manage files, and interact with Databricks components. Here's a brief overview of its main modules and functions:
  
  ### Key Modules and Functions of `dbutils`
  ![image](https://github.com/SandeepAnala1/Azure-Databricks/assets/163712602/207a1b16-eee0-46a5-8af0-51bf8c5e23c4)

  - **File System Utilities (`dbutils.fs`)**:
     - **Purpose**: To interact with the Databricks File System (DBFS).
     - **Common Functions**:
       - `ls(path)`: Lists files and directories in the specified path.
       - `mkdirs(path)`: Creates a directory at the specified path.
       - `rm(path, recurse)`: Deletes a file or directory at the specified path. If `recurse` is true, it deletes the directory and all its contents.
       - `cp(src, dst, recurse)`: Copies a file or directory from source to destination. If `recurse` is true, it copies directories recursively.
       - `mv(src, dst)`: Moves a file or directory from source to destination.

`abfss` stands for Azure Blob File System Secure, which is a URI scheme used to access Azure Data Lake Storage Gen2 in a secure manner. It is a secure extension of the `abfs` (Azure Blob File System) scheme, providing encrypted connections (via HTTPS) to Azure Data Lake Storage Gen2.

### Breakdown of the URI

The format `abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/` can be broken down as follows:

- `abfss://`: Indicates that the connection uses Azure Blob File System Secure (encrypted connection).
- `<container-name>`: The name of the container within the Azure Data Lake Storage Gen2 account.
- `<storage-account-name>`: The name of the Azure Storage account.
- `.dfs.core.windows.net`: The domain for accessing Azure Data Lake Storage Gen2.

Given a URI: `abfss://mycontainer@mystorageaccount.dfs.core.windows.net/`

- `mycontainer`: The container name within the storage account.
- `mystorageaccount`: The storage account name.
- `dfs.core.windows.net`: Indicates the endpoint for Azure Data Lake Storage Gen2.

### Usage in Databricks
In Databricks, you can use `abfss` URIs to mount and access data stored in Azure Data Lake Storage Gen2. For example, when configuring access to a storage account, you might use the `abfss` URI to specify the source path for data processing or analysis tasks.
- So here you need Access key & storage account name to retrieve the data from the specific container

You can find code [here](https://github.com/SandeepAnala1/Azure-Databricks/blob/main/2_Databricks-Architecture%20%2B%20Storage%20Account%20Access/1.Access%20Key.ipynb)
----------------------------------------------------------

## 2) SAS Key
  ![image](https://github.com/SandeepAnala1/Azure-Databricks/assets/163712602/57fe6fd7-ce83-43e9-a8f3-166fefe1a163)

  ![image](https://github.com/SandeepAnala1/Azure-Databricks/assets/163712602/02fc83f3-76b3-42fd-83bc-1278290ff4aa)

You can find code [here](https://github.com/SandeepAnala1/Azure-Databricks/blob/main/2_Databricks-Architecture%20%2B%20Storage%20Account%20Access/2.SAS%20Key.ipynb)
----------------------------------------------------------

## 3) Service Principle
- What is SP? -- It's an identity managed by customer which can be used for access
- To create it --> Microsoft Entra ID --> App registrations --> New --> Register
![image](https://github.com/SandeepAnala1/Azure-Databricks/assets/163712602/bfb8fab7-6829-485a-8718-8fb98402107e)

- It has Tenant ID & Client ID
- **Client ID**: The unique identifier assigned to an Azure Service Principal to represent it during authentication.
- **Tenant ID**: The unique identifier of the Azure Active Directory (AAD) instance (tenant) where the Service Principal is registered.
- Now we need to create secrets
  ![image](https://github.com/SandeepAnala1/Azure-Databricks/assets/163712602/21790876-0096-4284-bd6c-dd6a4624aade)
  ![image](https://github.com/SandeepAnala1/Azure-Databricks/assets/163712602/9a687986-78a7-4254-b9eb-9f5627c118f6)

- In Client secret, we need to add the value here

  ![image](https://github.com/SandeepAnala1/Azure-Databricks/assets/163712602/bbb8228e-b18e-4e49-b7e3-6796c8a12958)
The rest of the Hands-on you can find in this file
- Even after doing all this it fails to retrieve here
  ![image](https://github.com/SandeepAnala1/Azure-Databricks/assets/163712602/4c98775e-bb13-4607-97c4-00f50c28a46b)
- Because Service principle is just authentication, authorization we have to provide from IAM

-----------------------------------------------------------

## 4) IAM
  ![image](https://github.com/SandeepAnala1/Azure-Databricks/assets/163712602/09696b24-5f98-4cbb-8468-a4cf3ed4e5dd)

- From SA we need to add the Role Assignments
  ![image](https://github.com/SandeepAnala1/Azure-Databricks/assets/163712602/c6607845-2796-469d-8a6b-9fbe44dd0bf7)

- From here we need to choose Storage BLOB Data Contributor

  ![image](https://github.com/SandeepAnala1/Azure-Databricks/assets/163712602/cd1d5d3f-7759-4ff6-a373-926daea0fda4)

- Now it worked
  ![image](https://github.com/SandeepAnala1/Azure-Databricks/assets/163712602/3dc04f80-9a20-469c-8210-04ce978a6e49)

You can find code [here](https://github.com/SandeepAnala1/Azure-Databricks/blob/main/2_Databricks-Architecture%20%2B%20Storage%20Account%20Access/3.Service%20Principle.ipynb)

------------------------------------------------------------

## 5) Mounting
- We have 2 problems now, we we have multiple containers and when it required seperate session, we need to provide credentials again and again
- And also we need to provide full URL for storage account which is this `abfss://mycontainer@mystorageaccount.dfs.core.windows.net/`
- To over come this we did mounting and you can find code with below link

![image](https://github.com/SandeepAnala1/Azure-Databricks/assets/163712602/7b22c647-e87a-4ae3-b8da-cb30bc6c385e)

- This can be used outside this session and it will work


You can find code [here](https://github.com/SandeepAnala1/Azure-Databricks/blob/main/2_Databricks-Architecture%20%2B%20Storage%20Account%20Access/4.Mounting.ipynb)





























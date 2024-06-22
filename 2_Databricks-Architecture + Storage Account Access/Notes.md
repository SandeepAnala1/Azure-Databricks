### If you gone through spark internals, you can find so much information and calculations on memories in spark submit. During 2014, people used to submit their jobs by spark submit. So, Databricks came with the managed service where all the struggle of spark submit i.e., calculating the executor memory, driver memory for each job is taken care by Databricks itself.

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

1) Accessing it through Access key
  - Security + Networking -> Access Key
  ![image](https://github.com/SandeepAnala1/Azure-Databricks/assets/163712602/8797e7f0-477e-4893-b547-99fba573149b)























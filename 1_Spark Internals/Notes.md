# Why Spark has emerged?
- Back in 2004, we just have traditional databases which has only structured data, as the data started growing there is a need for faster processing
- The idea of "big data" began to gain traction as organizations recognized the limitations of traditional RDBMS systems in handling massive data volumes and new data types.
- Google's came up with MapReduce paper (published in 2004) and this lead to development of Hadoop at Yahoo! in the mid-2000s marked the beginning of a shift towards distributed data processing and storage.

  ## For any kind of data these are essential criterias
  - Scalability: How do you scale the data
  - Fault Tolerance
  - Cost-Effectiveness

  > To scale the data, you need to add the additional machines to servers which required down time. You need to hold on the process generally, it's tedious job

  ![image](https://github.com/SandeepAnala1/Azure-Databricks/assets/163712602/10bc617e-1009-4205-bcea-542bde666e37)


  ## Above issue lead to Distributed approach
  - With this approach, all the criterias are the advantages.
  - But think of it, as we are adding machines to the distributed approach, why is it cost effective?
  - Because, we are not adding the extension machines again and again like how we used to add for monolithic approach
    
  ![image](https://github.com/SandeepAnala1/Azure-Databricks/assets/163712602/bfae2507-a901-483f-bb75-17e8dc683ab7)

# This lead to the evolution of Hadoop & Hadoop Distributed File System
## Evolution of Hadoop

- **Google's MapReduce and GFS:** The story of Hadoop begins with Google's publication of the MapReduce framework and the Google File System (GFS). In 2004, Google released a paper that described these technologies. Hadoop   was created as an open-source implementation of MapReduce and Hadoop Distributed File System (HDFS) inspired by GFS.
  
- **Hadoop Core (2006):** Hadoop emerged in 2006 as an open-source project within the Apache Software Foundation. It consisted of the Hadoop Distributed File System (HDFS) for distributed storage and Hadoop MapReduce for distributed data processing. Main purpose of distribution is processing the data in parallel
  
- **Hadoop Ecosystem Growth (2007-2010):** During this period, Hadoop's ecosystem began to grow significantly. Various sub-projects and tools were developed to enhance its capabilities. Projects like Hive, Pig, HBase, and Zookeeper became part of the Hadoop ecosystem.

------------------------------------------------------------------------------
# Interesting question: What is the need of Operating System in a machine?
1) Managing Resources
2) Convert human understandable language to Machine understandable language
3) To process big data, we have the machines but how do they talk to each other & how do we get the feeling of working on a single machine?
### Here comes the Cluster Manager - **YARN**
-------------------------------------------------------------------------------

- **Hadoop 2.x and YARN (2012):** Hadoop 2.x marked a major evolution in the Hadoop ecosystem with the introduction of YARN (Yet Another Resource Negotiator). YARN decoupled the resource management and job scheduling capabilities from the MapReduce framework. This allowed Hadoop to support other data processing frameworks, making it more versatile.
  
- **Example:** Divide 1GB data into smaller chunks & do transformations, during monolithic approach it is performing all the transformations on 1GB. Obviously it gonna take more time. So with this idea, Hadoop became a hero of Big Data Industry and ruled for sometime untill the OG came into picture which is Apache Spark.
  
    > Quick node: 128 MB is the default partition in hadoop. 1GB divided to 8 smaller 128 MB chunks

  ![image](https://github.com/SandeepAnala1/Azure-Databricks/assets/163712602/f67a62fb-2aaa-42fb-b73a-11215d2406fc)

- We can even use HDFS for Monolithic databases to distribute the data, but processing cannot be happened. So to process this we have **Map/Reduce** - Mapper/Reducer 

# Database vs Hadoop
![image](https://github.com/SandeepAnala1/Azure-Databricks/assets/163712602/269bb02f-70f7-491c-bf08-fa80114a5365)

- Hive: It is an abstraction over distribution file system. Abstraction is needed to write the SQL like queries called HQL.
- 
----------------------------------------------------------
### For any platform, it can be hadoop/apache spark or any other tech, to make it faster it needs to things
i. Storage: Where we store our data, HDFS | Cloud Object Store(ADLS etc)
ii. Processing/computation: It means transformations, how do we do the transformations on Big data

### This processing is the issue with the Hadoop
![image](https://github.com/SandeepAnala1/Azure-Databricks/assets/163712602/2bda0b10-fb31-4dc5-a722-450589ab88fc)

- No caching: it is not in memory processing, any time we do the process, we write the data to disk and read from the disk again
- This lead to the rise of Apache Spark
-----------------------------------------------------------

# Apache Spark
Apache Spark is a powerful tool used to handle and analyze large amounts of data very quickly. Imagine you have a huge collection of books and you want to find specific information from all of them. Doing it by hand would take forever, but with Apache Spark, it's like having a team of super-fast readers who can look through all the books at once and give you the information you need in no time.

Here's a simple breakdown of how it works:

1. **Big Data Processing**: Spark is designed to handle "big data," which means extremely large datasets that can't be processed by a single computer efficiently.

2. **Speed**: It's incredibly fast because it uses a technique called "in-memory computing." This means it stores data in the computer's memory (RAM) instead of on a disk, which speeds up the processing significantly.

3. **Distributed Computing**: Spark can distribute tasks across many computers (a cluster) working together. It's like dividing a big job among many workers, so the work gets done much faster.

4. **Versatility**: Spark isn't just for one kind of task. It can handle a variety of data processing tasks, such as:
   - **Batch Processing**: Analyzing large volumes of data at once.
   - **Stream Processing**: Analyzing data in real-time as it comes in.
   - **Machine Learning**: Creating models that can make predictions based on data.
   - **Graph Processing**: Analyzing relationships within data, like social networks.

5. **Ease of Use**: It provides easy-to-use interfaces for programming in languages like Python, Java, and Scala, making it accessible for developers with different backgrounds.

In summary, Apache Spark is a fast and versatile tool that helps organizations process and analyze massive amounts of data efficiently, enabling quicker insights and smarter decisions..

 > Let's know cluster first : The pool of computers connected together is called cluster. But it viewed as single system.
 > Core: Degree of parallelism for machine
 


- It moved the process into in memory

# Apache Spark Architechture
![image](https://github.com/SandeepAnala1/Azure-Databricks/assets/163712602/3089a81b-65b0-4426-b7a5-8deae7405155)

- YARN | Kubernetes | Mesos : Cluster Manager or OS
- Spark Engine : It has all the code or libraries to run the job
- Spark Core : Flexible to write code in 4 different languages
- Spark SQL | Streaming

## Spark submit
A command line tool that allows you to submit the Spark application to the cluster.
- Why this is needed? If I need to execute or process small portion of data, there is no use of distributing the data and allocating resources

  `./bin/spark-submit \
      --class <main-class> \
      --master <master-url> \
      --deploy-mode <deploy-mode> \
      --conf <key>=<value> \
      ... # other options
      <application-jar> \
      [application-arguments]`

![image](https://github.com/SandeepAnala1/Azure-Databricks/assets/163712602/0603a89a-b2b6-438a-b79e-3869d69278e8)

- cores: Threads/degree of parallelism
- memory: RAM
- executors: No of computers(CPU)

----------------------------------------------------------------------

# Driver & Worker nodes
### IT Company Hierarchy vs. Spark Components

1. **Project Manager --> Resource Manager & Cluster Manager**
   - **Project Manager**: Oversees project, allocates resources.
   - **Resource Manager**: Manages resources across applications.
   - **Cluster Manager**: Monitors and schedules resources in the cluster.

2. **Technical Lead --> Driver**
   - **Technical Lead**: 
     1. Requests resources from Project Manager.
     2. Breaks down work into tasks.
     3. Coordinates tasks among developers.
   - **Driver**: 
     1. Requests resources from Cluster Manager.
     2. Breaks job into tasks.
     3. Distributes tasks to Executors and coordinates execution.

3. **Developers --> Executors (Workers)**
   - **Developers**: Execute assigned tasks, work on project parts, report progress.
   - **Executors**: Execute tasks from the Driver, process data, report results.

----------------------------------------------------------------------------

# In detail explanantion - Spark Cluster & Runtime Architecture
﻿## Spark Cluster and Runtime Architecture
Driver- It is the container in which the main method of a spark application is executed. The driver converts your Spark application into one or more Spark jobs. It then transforms each job into a DAG (Spark's execution plan), where each node within a DAG could be single or multiple Spark stages.
As soon as the Spark Application is submitted, cluster Manager creates a Application Master Container and allocates the driver memory and cores.

![image](https://github.com/SandeepAnala1/Azure-Databricks/assets/163712602/1d82216f-d8ce-4143-95df-25d05c9f7669)

  - What does JVM does? The JVM (Java Virtual Machine) in the Application Master container manages the execution of the application's tasks, ensuring efficient resource utilization and task scheduling within the Apache Spark cluster.
  - The JVM (Java Virtual Machine) is used in the worker nodes to execute tasks and run Spark jobs, enabling the distributed processing of data across the cluster by providing a runtime environment for Spark's operations and ensuring efficient resource management and task execution.

![image](https://github.com/SandeepAnala1/Azure-Databricks/assets/163712602/8843e47a-00a8-4c83-a3a1-c292e81d170b)

Sure, let's break down Apache Spark's cluster and runtime architecture, and the role of the JVM, in a simple way.

### Spark Cluster and Runtime Architecture

Think of a Spark cluster as a big team of workers managed by a supervisor:

1. **Driver Program**: This is like the team leader who plans the work and sends out tasks. It's the main program you write that tells Spark what to do.

2. **Cluster Manager**: This is the supervisor who assigns tasks to workers. Spark can use different types of cluster managers, like YARN, Mesos, or its own Standalone Cluster Manager.

3. **Workers (Executor)**: These are the team members who do the actual work. Each worker node in the cluster runs tasks assigned by the driver.

4. **Application Master**: This is a special role in some cluster managers (like YARN) that handles the execution of the application.

### Role of the JVM

The JVM (Java Virtual Machine) is a part of the setup on both the driver and the worker nodes. Here's what it does:

- **Driver Node**: The JVM runs the Driver Program, which coordinates all the work.
- **Worker Nodes**: The JVM runs the tasks sent by the Driver, processing the data as specified.

The JVM allows Spark to run on any machine that has a JVM installed, providing a consistent environment for executing tasks.

### Cluster Deploy Modes

1. **Cluster Mode**: The driver runs on one of the worker nodes. This mode is useful for production environments where you want the job to keep running independently after you submit it.

   - **When to use**: When running jobs in a production environment on a cluster where the driver does not need to be tightly connected to the client machine.
    ![image](https://github.com/SandeepAnala1/Azure-Databricks/assets/163712602/ae5aecdb-7db7-4423-81c5-0e79eec871d0)


2. **Client Mode**: The driver runs on the machine from which the job is submitted. The driver needs to be connected to the cluster for the duration of the job.

   - **When to use**: For development and testing, or when the client machine needs to interact closely with the job.

    ![image](https://github.com/SandeepAnala1/Azure-Databricks/assets/163712602/19a1d601-ff74-428d-b872-8323ca037f2b)

3. **Local Mode**: Everything runs on a single machine (both the driver and the executor tasks). This mode is typically used for development, testing, or debugging.

   - **When to use**: When developing and testing your Spark application on your local machine.

### Summary

- **Driver Program**: Plans and coordinates work.
- **Cluster Manager**: Assigns tasks to worker nodes.
- **Workers**: Execute tasks and process data.
- **JVM**: Runs both the Driver Program and tasks on worker nodes.

### Cluster Deploy Modes

- **Cluster Mode**: Driver on worker node.
- **Client Mode**: Driver on client machine.
- **Local Mode**: Everything on a single machine.

The JVM ensures that Spark applications can run consistently across different machines and environments, handling the execution of tasks and management of resources in real-time.













































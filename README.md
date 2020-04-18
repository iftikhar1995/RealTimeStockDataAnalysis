# Real Time Stock Data Analysis

The main objective of this project is to design and implement a scalable big data system with well-designed data lake that is able to
ingest, transform and store real-time stocks data.

# Tools and Technologies:

Following are the tools and technologies used in the project:
- Apache NiFi
- Apache Kafka
- Apache Spark
- Apache Hadoop
- Apache Cassandra
- MongoDB

# Implemented Architecture

1- The data will be fetched from the source using Apache NiFi. After every 1 minuit, the Nifi will fetch the data from
the source and will publish it to Apache Kafka.
2- The Spark Application will then consume the data and convert it into a structured format.
3- The structured data will be stored in cassandra.
4- Then we'll transform the data and will put it into MongoDB.
5- The transformed data will be printed on the console as well.

![Real Time Stock Data Analysis Architecture](../master/ReadMe/RealTimeStockDataAnalysis.png)
 
# Prerequisite

- SBT should be installed on the development machine.
- Scala editor of your choice. For this project I'm using IntelliJ community edition.
- Scala version 2.11.12 

 # Project Structure

```
    RealTimeStockDataAnalysis
        |
        |- src
        |   |
        |   |- main
        |   |   |
        |   |   |- scala
        |   |   |   |
        |   |   |   |- DataPipelineConfiguration.scala
        |   |   |   |- StockAnalysisDataPipeline.scala
        |
        |- build.sbt
        |- .gitignore
```

- **RealTimeStockDataAnalysis:** The main folder of the project.
    - **src/main/scala:** The folder containing scala files containing the implementation of datapipeline.
        - **DataPipelineConfiguration.scala:** The scala file containing the configuration required for fetching the 
        data from the Kafka and storing the data into Cassandra and MongoDB.
        - **StockAnalysisDataPipeline.scala:** The scala file contains the logic of transformation of stocks data. Also,
        this class will be responsible for storing data into Cassandra and MongoDB.
        - **build.sbt:** The file contains the dependencies required for the project

# Getting Started

Following instructions will get you a copy of the project up and running on your local machine for development and testing 
purposes.

1. Clone the repository using below command:\
   ```git clone <https://github.com/iftikhar1995/RealTimeStockDataAnalysis.git>```

2. Following are the steps to **setup project in IntelliJ**:
   1. Open the cloned project in IntelliJ or your preferred editor.
   2. Build the sbt project
   3. In the  **DataPipelineConfiguration.scala** add the required configurations of Apache Kafka, Apache Casandra & 
   MongoDB.
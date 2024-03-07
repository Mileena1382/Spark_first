                                             Kafka
Apache Kafka is an open-source distributed event streaming platform used for building real-time data pipelines and streaming applications. Kafka acts as a central hub where data streams from various sources can be collected and stored for further processing.
1. Workflow of Kafka,Spark and Druid
* Data Ingestion: Data is ingested into Kafka from various sources such as web servers, IoT devices, sensors, etc. Kafka acts as a centralized message broker for these streams of data.
* Data Ingestion: Data is ingested into Kafka from various sources such as web servers, IoT devices, sensors, etc. Kafka acts as a centralized message broker for these streams of data.
* Data Storage and Querying with Druid: The processed data from Spark can be stored in Druid for real-time analytics. Druid is particularly useful for aggregating and querying large volumes of data in real-time. Interactive dashboards or applications can then query Druid for insights or perform ad-hoc analysis on the processed data.
2. Components
* Producer: The producer is responsible for publishing data to Kafka topics. It sends records to Kafka brokers.
* Broker: Kafka brokers are servers that store and manage the data. They are responsible for receiving messages from producers, storing them, and serving them to consumers.
* Consumer: Consumers read data from Kafka topics. They subscribe to one or more topics and process the messages stored in the brokers.
* Topic: A topic is a category or feed name to which records are published by producers and from which consumers read records. It's similar to a queue in traditional messaging systems.
* Partitions : Topics in Kafka can be divided into one or more partitions.Kafka replicates partitions across multiple brokers for fault tolerance. This ensures that data is not lost even if some brokers fail.
Concepts
* Offsets : The messages within each partition are ordered and assigned a sequential offset.(a number 0,1...)
  
    

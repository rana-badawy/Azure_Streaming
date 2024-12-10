# Real-Time Streaming Project with Azure

This project demonstrates a real-time data streaming pipeline using Azure services. It simulates data generation, ingestion, and processing with a focus on real-time capabilities.

## Project Overview

### Workflow Steps
1. **Data Generation**:
   - Dummy data is generated every 10 seconds using the Python Faker library on a local machine.

2. **Data Ingestion**:
   - The generated data is sent to **Azure Event Hub** for reliable and scalable ingestion.

3. **Bronze Layer (Raw Data Storage)**:
   - Data from Event Hub is streamed into **Azure Data Lake Storage** Bronze container using **Azure Stream Analytics**.

4. **Silver Layer (Cleansed Data)**:
   - Data is processed and transformed in real-time using **Apache Spark Structured Streaming** in **Azure Databricks**, and then stored in the Silver layer of ADLS.

5. **Gold Layer (Aggregated Data)**:
   - Further transformations are applied in real-time using Spark in Databricks, and the data is stored in the Gold layer of ADLS.

6. **Data Aggregation**:
   - Data from the Gold layer is queried and aggregated in **Azure Synapse Analytics** using Spark pools.


## Prerequisites

- **Azure Subscription**: Access to Azure services.
- **Python**: Installed on your local machine.


## Azure Resources Used

- **Azure Event Hub**: For data ingestion.
- **Azure Data Lake Storage (Gen2)**: For storing raw, processed, and aggregated data.
- **Azure Stream Analytics**: For moving data from Event Hub to the Bronze layer.
- **Azure Databricks**: For real-time data transformation.
- **Azure Synapse Analytics**: For querying and aggregating data.

## Project Setup

### Step 1: Clone the Repository
  - Download the notebooks and the code to generate the data

### Step 2: Set Up Azure Resources
1. **Create Azure Event Hub**:
   - Follow the Azure Event Hub documentation to create an Event Hub namespace and an Event Hub instance.

2. **Set Up Data Lake Storage**:
   - Create a Data Lake Storage Gen2 account with containers for Bronze, Silver, and Gold layers.

3. **Configure Azure Stream Analytics**:
   - Set up a Stream Analytics job to read data from Event Hub and write it to the Bronze container in Data Lake Storage.

4. **Set Up Azure Databricks**:
   - Create a Databricks workspace.
   - Import the notebooks for Silver and Gold layer transformations.

5. **Configure Azure Synapse Analytics**:
   - Create a Synapse workspace and configure Spark pool to aggregate the data.


## Running The Project
  - Run the file **data_streaming.py**.
  - Run Stream Analytics Job
  - Run the transformation notebooks in Databricks
  - Run the aggregation notebook in Synapse Analytics

# Azure Data Engineering Project: Tokyo Olympic Data Pipeline

This project demonstrates a comprehensive data engineering pipeline using various Azure services to manage and analyze data from the Tokyo Olympics. The pipeline covers data ingestion, storage, transformation, and querying for insights.

## Project Components

- **Azure Storage Account**: Manages data storage.
- **Azure Data Factory**: Orchestrates and automates data flows.
- **Azure Databricks**: Provides a platform for data processing using Spark.
- **Azure Synapse Analytics**: Enables large-scale data querying.

## Architecture

1. **Storage Account**
    - A custom-named Storage Account to manage containers and directories for storing raw and transformed data.

2. **Azure Data Factory**
    - A pipeline is created within the same resource group as the storage account.
    - Datasets like Coaches and Athletes are set up with sources and sinks configured, utilizing linked services for connection without manual coding.

3. **Azure Databricks**
    - Setup involves creating a single-node cluster and mounting Azure Data Lake Storage for Spark processing.
    - An App Registration setup allows secure interaction with Azure resources.

4. **Azure Synapse Analytics**
    - Data is loaded from Databricks into Synapse Analytics tables for querying and analysis.

## Setup Guide

### Step 1: Storage Setup
- Create a Storage Account.
- Within the account, create a container and establish directories for `raw-data` and `transformed-data`.

### Step 2: Data Factory Configuration
- Under the same Resource Group as the Storage Account, set up an Azure Data Factory.
- Create a pipeline and define datasets with their respective sources and sinks.

### Step 3: Databricks Configuration
- Set up a Databricks workspace and create a single-node cluster.
- Mount your Azure Data Lake Storage for access in Spark-based notebooks.

### Step 4: Data Transformation and Loading
- Utilize Databricks notebooks for applying transformations to the data.
- Load transformed data into Azure Synapse Analytics for further processing and querying.

## Security and Authentication
- Ensure appropriate security measures and authentication methods are in place for interacting between services, such as using secure application registrations and managing secrets responsibly.

## Conclusion
This project layout serves as a template for building scalable data engineering pipelines using Azure services. It highlights how to leverage different Azure components seamlessly for effective data management and analysis.


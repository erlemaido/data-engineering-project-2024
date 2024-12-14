# Financial Data Pipeline

## How to Run the Project

### Prerequisites

1. Docker: Ensure you have Docker installed on your system.
2. Clone the Repository:
    ```
    git clone https://github.com/erlemaido/data-engineering-project-2024.git
    cd data-engineering-project-2024
    ```

### Steps to Run

1. Start the Services:

   `docker compose up`
   
   This will initialize the following services:

   * Airflow
   * MinIO
   * Iceberg REST
   * MongoDB 
   * DuckDB
   * Spark-Iceberg
   * dbt
   * Streamlit

2. Access Airflow: Navigate to http://localhost:8080. Log in with the default credentials (airflow/airflow).
3. Trigger the DAG manually or wait for their schedules to execute: 
   * process_monthly_financial_data_dag for processing financial, general and tax data. 
   * aggregate_audit_data_dag for aggregating audit data (Works only with Mac! Couldn't get spark-submit work on Windows.)
4. Access Streamlit: Navigate to [http://localhost:8501](http://localhost:8501/) to explore dashboards, data visualizations and ML outputs. No credentials needed

### Verifying Outputs

* Data Processing Outputs:
  * The processed .parquet files are uploaded to MinIO under the bucket.
* dbt Transformations:
  * Verify transformed tables and reports in DuckDB.
* MongoDB Aggregates:
  * Audit aggregates are stored in MongoDB. 

## Short Overview of What Happens

This project automates the ingestion, processing, and transformation of financial data files. Key steps include:

1. Data Ingestion: Files are retrieved from a public S3 bucket
2. Data Processing: Files are processed using DuckDB and saved into Iceberg tables.
3. Data Transformation: dbt is used to create a star schema
4. Processing: Spark jobs read and process data from Iceberg tables for aggregation. 
5. Data Storage: Aggregates are stored in MongoDB. Transformed data is archived in DuckDB for analytics. 

## Detailed Workflow

1. File Retrieval:
   * CSV files are listed and downloaded from public S3 bucket using the list_files and download_file functions. The input files are categorized into three types:
     * Fiscal Year Reports General Data: Contains metadata about the entities, including their legal form, status, fiscal year details, and submission dates.
     * Fiscal Year Reports Financial Data: Contains detailed financial performance metrics such as revenue, profit/loss, assets, liabilities, and other key financial indicators.
     * Tax Data: Provides information about tax payments, revenue, employee taxes, and related entity attributes.
   * The pipeline ensures that only relevant files with the .csv extension and matching prefixes are processed.
2. File Processing:
   * Each downloaded file is processed in DuckDB, which:
     * Reads the CSV files.
     * Configures DuckDB for S3 connectivity using credentials.
     * Creates temporary DuckDB tables for data processing.
   * Afterward, the data is converted into PyArrow tables where schema enforcement is applied, and additional columns such as year and quarter are appended based on the filenames.
3. Iceberg Table Management:
   * A namespace and table are created in Iceberg if they don’t already exist.
   * The processed Arrow table is appended to the Iceberg table using PyIceberg.
4. Processed File Upload:
   * Processed .parquet files are saved locally and uploaded to MinIO for archival and future use.
5. Spark Aggregations:
   * Apache Spark processes datasets using distributed computing to aggregate data.
   * Spark reads data directly from Iceberg tables and performs transformations using Spark SQL.
6. MongoDB Storage:
    * MongoDB stores aggregated data, such as audit counts by fiscal year and audit status.
    * After processing with Spark, aggregated .csv results are parsed and inserted into MongoDB.
    * Aggregates are used for quick access
7. dbt Transformation:
   * The dbt project is triggered via a BashOperator in Airflow.
   * dbt transforms the processed data into a star schema comprising:
     * Entity Dimension
     * Date Dimension
     * Fy Report Dimension
     * Financial Performance Fact
     * Tax Fact
8. Data Validation and Analytics:
   * The final transformed tables are stored in DuckDB for validation and analytics.
   * MongoDB aggregates are used for efficient querying of high-level metrics.
   * Overviews, proof-of-concept ML and access to raw data are accessible trough Streamlit

# About Project

## Problem statement
All legal entities in Estonia are registered in the Estonian Business Register. Most of them are required to submit annual fiscal year reports to the register. Legal entities over a certain threshold will need to have their reports audited before submission. These thresholds were last updated in 2017.
With inflation increasing rapidly over the last few years, the number of entities needing to submit audited reports has also increased, putting a strain on the limited capacity of auditors and creating delays in annual
report submissions.

## Data

Following data sources are used:
* Open data tables from the Estonian Tax and Customs Board
  * Taxes paid, turnover and size of workforce – quarterly data for years 2020-2024.
  The data is available in downloadable files in .csv format.
* Open data from Estonian e-Business Register 
  * General information of reports (until 31.10.2024) - annual reports data
  * Yearly reports: key indicators - annual fiscal report data for years 2020 - 2023


## Business Questions

* Has the number of legal entities required to submit audited reports risen between 2016-2023?
* Can an auditing requirement be predicted on tax data before the fiscal year ends (on 3 or less quarters of tax data)?
* Has the auditing requirement and fixed thresholds impacted any specific field of activity more than others?



## Business Requirements

* Language Standardization: All original data, regardless of its initial language (Estonian or English), should be standardized to English. This includes translating all field names to English to maintain consistency across the database.
* Currency Representation: All currency values related to fiscal year report data should be recorded in whole Euros, omitting any cents. 

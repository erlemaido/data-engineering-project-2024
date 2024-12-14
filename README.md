# Financial Data Pipeline

## How to Run the Project

### Prerequisites

1. Docker: Ensure you have Docker installed on your system.
2. Minimum RAM: 32GB. Some file processing tasks (e.g., large .csv file handling) may fail with less than 32GB RAM due to high memory requirements.
3. Clone the Repository:
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
  * Use the following command to connect to the MongoDB container: 
  `docker exec -it data-engineering-project-2024-mongodb-1 mongosh`
  * Run this to check the contents: 
    ```
    use default

    var collections = db.getCollectionNames();
    for(var i = 0; i< collections.length; i++){
        print('Collection: ' + collections[i]); 
        db.getCollection(collections[i]).find().forEach(printjson); 
    }
    ```
  * Example document format:
    ```
    {
        _id: '2023_audited_Jah',
        value: '10019'
    }
    {
        _id: '2023_audited_Ei',
        value: '236784'
    }
    ```

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
     * Creates temporary DuckDB tables for data processing.
   * Afterward, the data is converted into PyArrow tables where schema enforcement is applied, and additional columns such as year and quarter are appended based on the filenames.
3. Iceberg Table Management:
   * A namespace and table are created in Iceberg if they don’t already exist.
   * The processed Arrow table is appended to the Iceberg table using PyIceberg.
4. Processed File Upload:
   * Iceberg tables are exported to .parquet files and uploaded to MinIO for archival and future use. 
5. Spark Aggregations:
   * Apache Spark processes datasets using distributed computing to aggregate data.
   * Spark reads data directly from Iceberg tables and performs transformations using Spark SQL.
6. MongoDB Storage:
    * MongoDB stores aggregated data, such as audit counts by fiscal year and audit status.
    * After processing with Spark, aggregated .csv results are parsed and inserted into MongoDB.
    * Aggregates are used for quick access.
7. dbt Transformation:
   * The dbt project is triggered via a BashOperator in Airflow.
   * dbt transforms the processed data into a star schema comprising:
     * Entity Dimension
     * Date Dimension
     * Fy Report Dimension
     * Financial Performance Fact
     * Tax Fact
     * ![image](https://github.com/user-attachments/assets/a196237e-b5bd-42a0-b414-6455997ba2ff)

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


# Notes

## DBT and Iceberg

The current implementation for data source and output of dbt is DuckDB, but we also experimented with the possibility of using Iceberg. 
The Iceberg implementation uses Spark as the query engine, just like Airflow did. Unfortunately we had to keep on using DuckDB, because we implemented the Streamlit solution on top of DuckDB.
We recognise that the DuckDB solution is technically substandard compared to the Spark + Iceberg solution, due to scalability issues with the single .parquet export.

The following snippets of code are included to show how we would have done it with the Spark + Iceberg way.

This should be in the profiles.yml
```
iceberg:
  target: local
  outputs:
    local:
      type: spark
      method: session
      host: spark-iceberg
      schema: staging
      port: 7077
      server_side_parameters:
        spark.sql.extensions: org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
        spark.sql.defaultCatalog: rest
        spark.sql.catalog.rest: org.apache.iceberg.spark.SparkCatalog
        spark.sql.catalog.rest.catalog-impl: org.apache.iceberg.rest.RESTCatalog
        spark.sql.catalog.rest.uri: http://iceberg_rest:8181/
        spark.hadoop.fs.s3a.endpoint: http://minio:9000
        spark.sql.catalog.rest.warehouse: s3a://warehouse
        spark.hadoop.fs.s3a.impl: org.apache.hadoop.fs.s3a.S3AFileSystem
        spark.sql.catalog.rest.io-impl: org.apache.iceberg.aws.s3.S3FileIO
        spark.sql.catalog.rest.s3.endpoint: http://minio:9000
        spark.executorEnv.AWS_REGION: us-east-1
        AWS_REGION: us-east-1
        spark.jars.packages: org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.80.0,software.amazon.awssdk:bundle:2.24.8,software.amazon.awssdk:url-connection-client:2.24.8
```

In order to switch between implementations, there is a need to configure the input source.
```
SELECT *
{% if var('database') == 'iceberg' %}
FROM staging.tax_data
{% else %}
FROM read_parquet('s3://bucket/tax_data.parquet')
{% endif %}
```

The dbt job should be run with: `dbt run --vars '{database: iceberg}' --profile iceberg`

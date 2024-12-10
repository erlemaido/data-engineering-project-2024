from pyspark.sql import SparkSession

spark = (SparkSession.builder.appName("IcebergTableRead")
         .getOrCreate())

database_name = "staging"
table_name = "report_general_data"

df = spark.read.format("iceberg").load(f"{database_name}.{table_name}")

df.show()

spark.sql("SELECT fiscal_year, audited, count(*) FROM staging.report_general_data group by fiscal_year, audited").show(truncate=False)

aggregate_df = spark.sql("SELECT fiscal_year, audited, count(*) FROM staging.report_general_data group by fiscal_year, audited")

aggregate_df.toPandas().to_csv('/opt/airflow/data/aggregate_res.csv')

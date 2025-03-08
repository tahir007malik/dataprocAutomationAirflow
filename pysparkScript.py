from pyspark.sql import SparkSession
from google.cloud import storage
from pyspark.sql.functions import *
from datetime import datetime
import os

# Configurations
bucket_name = os.getenv("BUCKET_NAME", "asia-south1-airflow-cluster-2918ac4f-bucket")
prefix = "dataprocAutomationAirflow/CSV_Data/"
output_path = "gs://asia-south1-airflow-cluster-2918ac4f-bucket/dataprodAutomationAirflow/Results/"

try:
    # Initialize GCS client
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix)

    # Find most recent blob
    most_recent_blob = max(blobs, key=lambda b: b.time_created, default=None)
    if not most_recent_blob:
        print("No files found in the specified directory.")
        exit()

    file_name = os.path.basename(most_recent_blob.name)
    print(f"Processing file: {file_name}")

    # Initialize Spark
    spark = SparkSession.builder.appName("dataprocAutomationAirflowJob").enableHiveSupport().getOrCreate()

    # Read CSV file
    file_path = f"gs://{bucket_name}/{prefix}{file_name}"
    data_Read = spark.read.csv(file_path, header=True, inferSchema=True)

    # Transformations
    # Top 3 Most Common Diseases
    a = data_Read.groupBy('diagnosis_description').agg(count('*').alias('Diseases_Count')).orderBy(desc('Diseases_Count'))

    # Flag for senior patients
    b = data_Read.withColumn('Flag', when(data_Read["age"] >= 60, 1).otherwise(0))

    # Age Category Disease wise
    c = spark.sql("""
    SELECT
        diagnosis_description,
        SUM(CASE WHEN age >= 30 AND age <= 40 THEN 1 END) AS `30-40`,
        SUM(CASE WHEN age >= 41 AND age <= 50 THEN 1 ELSE 0 END) AS `41-50`,
        SUM(CASE WHEN age >= 51 AND age <= 60 THEN 1 ELSE 0 END) AS `51-60`,
        SUM(CASE WHEN age >= 61 AND age <= 70 THEN 1 ELSE 0 END) AS `61-70`
    FROM health_Data
    GROUP BY diagnosis_description
    """)

    # Add processing_date
    processing_date = datetime.now().strftime("%Y-%m-%d")
    a = a.withColumn("processing_date", lit(processing_date))
    b = b.withColumn("processing_date", lit(processing_date))
    c = c.withColumn("processing_date", lit(processing_date))

    # Write outputs to GCS
    a.coalesce(1).write.mode("append").partitionBy("processing_date").parquet(output_path + "top3_diseases/")
    b.coalesce(1).write.mode("append").partitionBy("processing_date").parquet(output_path + "senior_flag/")
    c.coalesce(1).write.mode("append").partitionBy("processing_date").parquet(output_path + "age_category/")

    print("Data successfully appended to GCS with partitioning!")

    spark.stop()

except Exception as e:
    print(f"An error occurred: {e}")

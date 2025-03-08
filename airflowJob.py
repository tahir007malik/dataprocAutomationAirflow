from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitPySparkJobOperator,
    DataprocDeleteClusterOperator
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor

# Project and Bucket Configuration
PROJECT_ID = "joh9yyy-project-1"
BUCKET_1 = "asia-south1-airflow-cluster-2918ac4f-bucket"

# Default DAG Arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG Definition
dag = DAG(
    'dataproc_Automation_Airflow_DAG',
    default_args=default_args,
    catchup=False,
    start_date=datetime(2025, 3, 7),  # Explicit start date
    schedule_interval='@daily',
    tags=['dataproc_hive_airflow_automation'],
    description='A DAG to run Spark job on Dataproc',
)

# Cluster Configuration
CLUSTER_NAME = 'dataproc-cluster-75'
REGION = 'us-central1'
CLUSTER_CONFIG = {
    'master_config': {
        'num_instances': 1,
        'machine_type_uri': 'n1-standard-2',
        'disk_config': {'boot_disk_type': 'pd-standard', 'boot_disk_size_gb': 30},
    },
    'worker_config': {
        'num_instances': 2,
        'machine_type_uri': 'n1-standard-2',
        'disk_config': {'boot_disk_type': 'pd-standard', 'boot_disk_size_gb': 30},
    },
    'software_config': {'image_version': '2.0-debian10'},
}

# GCS Sensor Task
gcs_object_exists = GCSObjectsWithPrefixExistenceSensor(
    task_id="gcs_objects_exist_task",
    bucket=BUCKET_1,
    prefix="dataprocAutomationAirflow/CSV_Data/healthdata_",
    mode='poke',  # Poke mode ensures periodic checks
    timeout=43200,  # 12 hours timeout
    poke_interval=300,  # 5 minutes interval
    dag=dag,
)

# Create Dataproc Cluster Task
create_cluster = DataprocCreateClusterOperator(
    task_id='create_cluster',
    cluster_name=CLUSTER_NAME,
    project_id=PROJECT_ID,
    region=REGION,
    cluster_config=CLUSTER_CONFIG,
    dag=dag,
)

# Submit PySpark Job Task
pyspark_job = {
    'main_python_file_uri': 'gs://asia-south1-airflow-cluster-2918ac4f-bucket/dataprocAutomationAirflow/Scripts/pysparkScript.py'
}
submit_pyspark_job = DataprocSubmitPySparkJobOperator(
    task_id='submit_pyspark_job',
    main=pyspark_job['main_python_file_uri'],
    cluster_name=CLUSTER_NAME,
    region=REGION,
    project_id=PROJECT_ID,
    dag=dag,
)

# Delete Dataproc Cluster Task
delete_cluster = DataprocDeleteClusterOperator(
    task_id='delete_cluster',
    project_id=PROJECT_ID,
    cluster_name=CLUSTER_NAME,
    region=REGION,
    trigger_rule='all_done',  # Delete cluster even if the job fails
    dag=dag,
)

# Task Dependencies
gcs_object_exists >> create_cluster
create_cluster >> submit_pyspark_job
submit_pyspark_job >> delete_cluster

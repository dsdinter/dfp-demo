from airflow import utils
from airflow import DAG
from airflow import macros
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': utils.dates.days_ago(30),
    'email': ['dinterd@acme.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'provide_context': True,
}

# Set Schedule: Run pipeline once a day. Runs at end of day at 21:00.
# Use cron to define exact time. Eg. 8:15am would be "15 08 * * *"
schedule_interval = "00 21 * * *"

# Define DAG: Set ID and assign default args and schedule interval
dag = DAG('Process_Daily_NetworkImpressions_v1', default_args=default_args, schedule_interval=schedule_interval)

load_gcs_to_bq_daily_impressions = GoogleCloudStorageToBigQueryOperator(
    task_id='load_gcs_to_bq_daily_impressions',
    bucket='{{var.value.gcs_bucket}}',
    source_objects=[
        '{{var.value.gcs_root}}/*{{ ds_nodash }}.csv.gz'
    ],
    destination_project_dataset_table='{{var.value.bq_staging}}.csv_impressions_auto_schema{{ ds_nodash }}',
    source_format='CSV',
    skip_leading_rows=1,
    field_delimiter=',',

    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_TRUNCATE',
    src_fmt_configs={'autodetect': True},
    retries=0,
    dag=dag
)

flatten_append_to_impressions_facts = BigQueryOperator(
    task_id='flatten_append_to_impressions_facts',
    use_legacy_sql=False,
    write_disposition='WRITE_APPEND',
    allow_large_results=True,
    bql='''
    #StandardSQL
    SELECT
      fact.*, 
      dimension.ProductDescription
    FROM
      `{{var.value.bq_staging}}.csv_impressions_auto_schema{{ ds_nodash }}` as fact,
      `dwh.product_match_table` as dimension
    WHERE
      time>'{{ yesterday_ds }}'
      AND time<'{{ ds }}'
      AND fact.Product = dimension.Product
    ''',
    destination_dataset_table='dwh.impressions_flatten${{ yesterday_ds_nodash }}',
    retries=0,
    dag=dag)

flatten_append_to_impressions_facts_yesterday = BigQueryOperator(
    task_id='flatten_append_to_impressions_facts_yesterday',
    use_legacy_sql=False,
    write_disposition='WRITE_APPEND',
    allow_large_results=True,
    bql='''
    #StandardSQL
    SELECT
      fact.*, 
      dimension.ProductDescription
    FROM
      `{{var.value.bq_staging}}.csv_impressions_auto_schema{{ ds_nodash }}` as fact,
      `dwh.product_match_table` as dimension
    WHERE
      time>'{{ macros.ds_add(ds, -2)}}' 
      AND time<'{{ yesterday_ds }}'
      AND fact.Product = dimension.Product
    ''',
    destination_dataset_table='dwh.impressions_flatten${{ yesterday_ds_nodash }}',
    retries=0,
    dag=dag)

flatten_append_to_impressions_facts_twodays_ago = BigQueryOperator(
    task_id='flatten_append_to_impressions_facts_twodays_ago',
    use_legacy_sql=False,
    write_disposition='WRITE_APPEND',
    allow_large_results=True,
    bql='''
    #StandardSQL
    SELECT
      fact.*, 
      dimension.ProductDescription
    FROM
      `{{var.value.bq_staging}}.csv_impressions_auto_schema{{ ds_nodash }}` as fact,
      `dwh.product_match_table` as dimension
    WHERE
      time>'{{ macros.ds_add(ds, -3)}}' 
      AND time<'{{ macros.ds_add(ds, -2)}}'
      AND fact.Product = dimension.Product
    ''',
    destination_dataset_table='dwh.impressions_flatten${{ macros.ds_format(macros.ds_add(ds, -2), "%Y-%m-%d", "%Y%m%d")}}',
    retries=0,
    dag=dag)

# sens_object_create = GoogleCloudStorageObjectSensor(
#     task_id='sens_object_create',
#     bucket='{{var.value.gcs_bucket}}',
#     object='{{var.value.gcs_root}}/*{{ ds_nodash }}.csv.gz',
#     retries=0,
#     dag=dag
# )
#
# sens_object_update = GoogleCloudStorageObjectUpdatedSensor(
#     task_id='sens_object_update',
#     bucket='{{var.value.gcs_bucket}}',
#     object='{{var.value.gcs_root}}/*{{ ds_nodash }}.csv.gz',
#     retries=0,
#     dag=dag
# )
#
# sens_object_create >> load_gcs_to_bq_daily_impressions
# sens_object_update >> load_gcs_to_bq_daily_impressions

load_gcs_to_bq_daily_impressions >> flatten_append_to_impressions_facts
load_gcs_to_bq_daily_impressions >> flatten_append_to_impressions_facts_yesterday
load_gcs_to_bq_daily_impressions >> flatten_append_to_impressions_facts_twodays_ago

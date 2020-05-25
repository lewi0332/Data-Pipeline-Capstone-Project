from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import StageToRedshiftOperator
from airflow.operators import GetElasticsearchData
from airflow.operators import LoadFactOperator
from airflow.operators import DataQualityOperator
from helpers import SqlQueries


default_args = {
    'owner': 'derrick',
    'start_date': datetime.now() - timedelta(minutes=5),
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

dag = DAG('social-system-dag',
          default_args=default_args,
          description='Transform and Load data to Redshift with Airflow',
          schedule_interval='@daily',
          max_active_runs=1,
          )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_users_to_redshift = StageToRedshiftOperator(
    task_id='Stage_users',
    provide_context=True,
    dag=dag,
    table="Staging_users",
    conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="social-system-test/",
    s3_key="instagram_graph/stats",
    region="us-east-1",
)

get_ES_data = GetElasticsearchData(
    task_id='Get_Elasticsearch_data_aggregagations',
    provide_context=True,
    dag=dag,
    aws_credentials_id="aws_credentials",
    es_host="search-social-system-kkehzvprsvgkfisnfulapobkpm.us-east-1.es.amazonaws.com",
    es_index="instagram_graph_posts",
    days=60,
    region='us-east-1',
    service_type='es',

)
stage_aggregations_to_redshift = StageToRedshiftOperator(
    task_id='Stage_aggregations',
    provide_context=True,
    dag=dag,
    table="Staging_aggregations",
    conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="social-system-test/",
    s3_key="temp/post_agg_{run_id}.csv",
    region="us-east-1",
    file_type="csv"
)

load_history_table = LoadFactOperator(
    task_id='Load_history_fact_table',
    dag=dag,
    provide_context=True,
    conn_id='redshift',
    table='history',
    query=SqlQueries.get_profile_history,
    truncate=True,
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    provide_context=True,
    conn_id='redshift',
    tables=["history"]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_users_to_redshift
start_operator >> get_ES_data >> stage_aggregations_to_redshift
[stage_users_to_redshift, stage_aggregations_to_redshift] >> load_history_table
load_history_table >> run_quality_checks >> end_operator

from __future__ import print_function

import logging

import airflow
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.hooks.S3_hook import S3Hook
from airflow.operators.python_operator import PythonOperator

args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(7),
    'provide_context': True
}

dag = airflow.DAG(
    'images',
    schedule_interval='@once',
    default_args=args,
    max_active_runs=1)

# https://docs.aws.amazon.com/emr/latest/APIReference/API_RunJobFlow.html
default_emr_settings = {"Name": "image_classification",
                        "LogUri": "s3://social-system-test/spark/logs/",
                        "ReleaseLabel": "emr-6.0.0",
                        "Instances": {
                            "InstanceGroups": [
                                {
                                    "Name": "Master nodes",
                                    "Market": "ON_DEMAND",
                                    "InstanceRole": "MASTER",
                                    "InstanceType": "m5.xlarge",
                                    "InstanceCount": 1
                                },
                                {
                                    "Name": "Worker nodes",
                                    "Market": "ON_DEMAND",
                                    "InstanceRole": "CORE",
                                    "InstanceType": "m5.xlarge",
                                    "InstanceCount": 1
                                }
                            ],
                            "Ec2KeyName": "fohr_derrick",
                            "KeepJobFlowAliveWhenNoSteps": True,
                            'EmrManagedMasterSecurityGroup': 'sg-0af5e93985bbc0c65',
                            'EmrManagedSlaveSecurityGroup': 'sg-0d6786f4d41e1cd54',
                            'Placement': {
                                'AvailabilityZone': 'us-east-1a',
                            },

                        },
                        "BootstrapActions": [
                            {
                                'Name': 'install libraries to local',
                                'ScriptBootstrapAction': {
                                    'Path': 's3://social-system-test/spark/libraries.sh'
                                }
                            }
                        ],

                        "Applications": [
                            #{"Name": "Hadoop"}, change1
                            {"Name": "Spark"},
                            {"Name": "TensorFlow"},

                        ],
                        "VisibleToAllUsers": True,
                        "JobFlowRole": "EMR_EC2_DefaultRole",
                        "ServiceRole": "EMR_DefaultRole",
                        "Tags": [
                            {
                                "Key": "app",
                                "Value": "imageClassification"
                            },
                            {
                                "Key": "environment",
                                "Value": "development"
                            }
                        ]
                        }


def issue_step(name, args):
    return [
        {
            "Name": name,
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": args
            }
        }
    ]


def check_data_exists():
    logging.info('checking that data exists in s3')
    source_s3 = S3Hook(aws_conn_id='aws_credentials')
    keys = source_s3.list_keys(bucket_name='social-system-test',
                               prefix='instagram_graph/posts/')
    logging.info('keys {}'.format(keys))


check_data_exists_task = PythonOperator(task_id='check_data_exists',
                                        python_callable=check_data_exists,
                                        provide_context=False,
                                        dag=dag)

create_job_flow_task = EmrCreateJobFlowOperator(
    task_id='create_job_flow',
    aws_conn_id='aws_credentials',
    emr_conn_id='emr_default',
    job_flow_overrides=default_emr_settings,
    dag=dag
)


run_step = issue_step('run_spark_py', ["spark-submit", "--deploy-mode", "client", "--master",
                                       "yarn",
                                       "/home/hadoop/spark_image.py"])
# "--class", "org.apache.spark.examples.JavaLogQuery",

add_step_task = EmrAddStepsOperator(
    task_id='add_step',
    job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
    aws_conn_id='aws_credentials',
    steps=run_step,
    dag=dag
)

watch_prev_step_task = EmrStepSensor(
    task_id='watch_prev_step',
    job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull('add_step', key='return_value')[0] }}",
    aws_conn_id='aws_credentials',
    dag=dag
)

terminate_job_flow_task = EmrTerminateJobFlowOperator(
    task_id='terminate_job_flow',
    job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
    aws_conn_id='aws_credentials',
    trigger_rule="all_done",
    dag=dag
)

check_data_exists_task >> create_job_flow_task
create_job_flow_task >> add_step_task
add_step_task >> watch_prev_step_task
watch_prev_step_task >> terminate_job_flow_task

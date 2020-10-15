from datetime import timedelta

from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': days_ago(0),
    'catchup': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'funnel_pipeline',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    max_active_runs=1,
    concurrency=2
)

funnel_extraction_complete = DummyOperator(task_id="funnel_extraction_complete", dag=dag)

load_users_operator = SparkSubmitOperator(
    task_id='load_users_table_job',
    application='/Users/aleemr/powerhouse/interviews/jerry-coding-challenge/spark_jobs/load_users.py',
    dag=dag
)
load_users_operator.set_downstream(funnel_extraction_complete)

load_step1_operator = SparkSubmitOperator(
    task_id='load_step1_table_job',
    application='/Users/aleemr/powerhouse/interviews/jerry-coding-challenge/spark_jobs/load_step1.py',
    dag=dag
)
load_step1_operator.set_downstream(funnel_extraction_complete)

load_step2_operator = SparkSubmitOperator(
    task_id='load_step2_table_job',
    application='/Users/aleemr/powerhouse/interviews/jerry-coding-challenge/spark_jobs/load_step2.py',
    dag=dag
)
load_step2_operator.set_downstream(funnel_extraction_complete)

load_step3_operator = SparkSubmitOperator(
    task_id='load_step3_table_job',
    application='/Users/aleemr/powerhouse/interviews/jerry-coding-challenge/spark_jobs/load_step3.py',
    dag=dag
)
load_step3_operator.set_downstream(funnel_extraction_complete)

load_step4_operator = SparkSubmitOperator(
    task_id='load_step4_table_job',
    application='/Users/aleemr/powerhouse/interviews/jerry-coding-challenge/spark_jobs/load_step4.py',
    dag=dag
)
load_step4_operator.set_downstream(funnel_extraction_complete)

load_step5_operator = SparkSubmitOperator(
    task_id='load_step5_table_job',
    application='/Users/aleemr/powerhouse/interviews/jerry-coding-challenge/spark_jobs/load_step5.py',
    dag=dag
)
load_step5_operator.set_downstream(funnel_extraction_complete)

load_step6_operator = SparkSubmitOperator(
    task_id='load_step6_table_job',
    application='/Users/aleemr/powerhouse/interviews/jerry-coding-challenge/spark_jobs/load_step6.py',
    dag=dag
)
load_step6_operator.set_downstream(funnel_extraction_complete)

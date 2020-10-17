from datetime import timedelta

from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
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
    'dimension_modeling',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    max_active_runs=1,
    concurrency=2
)

wait_for_funnel_pipeline = ExternalTaskSensor(
    task_id='wait',
    external_dag_id='wait_for_funnel_pipeline',
    external_task_id='funnel_extraction_complete',
    dag=dag
)

start_dimension_modeling = DummyOperator(task_id="start_dimension_modeling", dag=dag)

wait_for_funnel_pipeline.set_downstream(start_dimension_modeling)

step1_fact_table = SparkSubmitOperator(
    task_id='step1_fact_table',
    application='/Users/aleemr/powerhouse/interviews/jerry-coding-challenge/spark_jobs/dimension_models/step1_fact_table.py',
    dag=dag
)

step2_fact_table = SparkSubmitOperator(
    task_id='step2_fact_table',
    application='/Users/aleemr/powerhouse/interviews/jerry-coding-challenge/spark_jobs/dimension_models/step2_fact_table.py',
    dag=dag
)

step3_fact_table = SparkSubmitOperator(
    task_id='step3_fact_table',
    application='/Users/aleemr/powerhouse/interviews/jerry-coding-challenge/spark_jobs/dimension_models/step3_fact_table.py',
    dag=dag
)

step4_fact_table = SparkSubmitOperator(
    task_id='step4_fact_table',
    application='/Users/aleemr/powerhouse/interviews/jerry-coding-challenge/spark_jobs/dimension_models/step4_fact_table.py',
    dag=dag
)

step5_fact_table = SparkSubmitOperator(
    task_id='step5_fact_table',
    application='/Users/aleemr/powerhouse/interviews/jerry-coding-challenge/spark_jobs/dimension_models/step5_fact_table.py',
    dag=dag
)

step6_fact_table = SparkSubmitOperator(
    task_id='step6_fact_table',
    application='/Users/aleemr/powerhouse/interviews/jerry-coding-challenge/spark_jobs/dimension_models/step6_fact_table.py',
    dag=dag
)

start_dimension_modeling.set_downstream(step1_fact_table)
start_dimension_modeling.set_downstream(step2_fact_table)
start_dimension_modeling.set_downstream(step3_fact_table)
start_dimension_modeling.set_downstream(step4_fact_table)
start_dimension_modeling.set_downstream(step5_fact_table)
start_dimension_modeling.set_downstream(step6_fact_table)

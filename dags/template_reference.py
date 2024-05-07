from datetime import datetime
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

@dag(schedule_interval="* * * * *", start_date=datetime(2024, 4, 1), catchup=False)
def template_reference():
    start_task = EmptyOperator(task_id="start_task")
    end_task   = EmptyOperator(task_id="end_task")

    def _demo_template(**kwargs):
        print("=== logical date ==>")
        print("data_interval_start:", kwargs['data_interval_start'])
        print("data_interval_end:", kwargs['data_interval_end'])

        print("=== get date and timestamp ==>")
        print("ds:", kwargs['ds'])
        print("ts:", kwargs['ts'])

        print("=== realtime ==>")
        print("dag_run_start_date:", kwargs['dag_run_start_date'])
        print("start_task_start_date:", kwargs['start_task_start_date'])

        print("=== transform ==>")

        print("data_interval_end_wib:", kwargs['data_interval_end_wib'])
        print("datetime_string:", kwargs['datetime_string'])


from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.operators.empty import EmptyOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator

@dag()
def etl_mysql_to_hive_dibimbing_karyawan():
    start_task = EmptyOperator(task_id="start_task")
    end_task   = EmptyOperator(task_id="end_task")

    extract = MySqlOperator(
        task_id       = "extract_from_mysql",
        mysql_conn_id = "mysql_default",
        sql           = "SELECT * FROM dibimbing.karyawan",
    )

    @task(task_id="transform_with_python")
    def transform():
        context = get_current_context()
        ti      = context["ti"]
        data    = ti.xcom_pull("extract_from_mysql")

        transformed = [[id+100, nama.upper()] for id, nama in data]
        print(transformed)

        return transformed


    load = HiveOperator(
        task_id          = "load_to_hive",
        hive_cli_conn_id = "hive_cli_default",
        hql              = """
            INSERT INTO dibimbing.karyawan (id, nama) VALUES
            {% for row in ti.xcom_pull(task_ids='transform_with_python') %}
                ({{ row[0] }}, '{{ row[1] }}')
                {% if not loop.last %}
                ,
                {% endif %}
            {% endfor %}
        """,
    )

    start_task >> extract >> transform() >> load >> end_task

etl_mysql_to_hive_dibimbing_karyawan()

















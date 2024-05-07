from airflow.decorators import dag, task_group
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.apache.hive.operators.hive import HiveOperator

list_table = Variable.get("mysql_to_hive", deserialize_json=True)

@dag()
def etl_mysql_to_hive_all():
    start_task = EmptyOperator(task_id="start_task")
    end_task   = EmptyOperator(task_id="end_task")

    for table in list_table:
        group_id = table.replace(".", "__")

        @task_group(group_id=group_id)
        def group():
            get_schema = MySqlOperator(
                task_id       = "get_schema",
                mysql_conn_id = "mysql_default",
                sql           = f"DESC {table}",
            )

            extract = MySqlOperator(
                task_id       = "extract_from_mysql",
                mysql_conn_id = "mysql_default",
                sql           = f"SELECT * FROM {table}",
            )



            load = HiveOperator(
                task_id          = "load_to_hive",
                hive_cli_conn_id = "hive_cli_default",
                hql              = """
                    TRUNCATE TABLE """+table+""";
               
                    INSERT INTO """+table+"""
                    (
                        {% for column in ti.xcom_pull(task_ids='"""+group_id+""".get_schema') %}
                            {{ column[0] }} {{ ', ' if not loop.last else '' }}
                        {% endfor %}
                    )
                    VALUES
                    {% for data in ti.xcom_pull(task_ids='"""+group_id+""".extract_from_mysql') %}
                        (
                            {% for column in data %}
                                {{ "'"~column~"'" if column is string else column }} {{ ', ' if not loop.last else '' }}
                            {% endfor %}
                        ) {{ ', ' if not loop.last else '' }}
                    {% endfor %}
                """,
            )

            start_task >> get_schema >> extract >> load >> end_task

        group()

etl_mysql_to_hive_all()




from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

@dag()
def template_reference_jinja():
    @task
    def send_data():
        return {
            "nama"       : ["soekarno", "soeharto", "habibie", "gusdur"],
            "tahun_lahir": [1901, 1921, 1936, 1940],
        }

    print_data = BashOperator(
        task_id      = "print_data",
        bash_command = """
            {% set data = ti.xcom_pull(task_ids='send_data') %}

            {% for nama in data['nama'] %}
                echo "Presiden {{ nama.upper() }} lahir tahun {{ data['tahun_lahir'][loop.index-1] }}";
            {% endfor %}
        """
    )

    send_data() >> print_data

template_reference_jinja()



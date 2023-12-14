from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from libs.operator.bigquery_operator import bigQueryOperator
from datetime import datetime

image_python = "vfdesouza/img-etl-faculdade:v1"

dag = DAG(
    "datapipeline_b3",
    schedule_interval=None,
    start_date=datetime(2023, 11, 1),
    catchup=False,
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 1,
    },
)

with dag:
    extraction_data_api_from_raw_zone = DockerOperator(
        task_id="extract_api_b3_daily",
        image=image_python,
        command="python -m scripts.extract_api_b3_daily",
    )

    # Tarefa para executar os scripts DBT
    process_dbt_tables_dim = DockerOperator(
        task_id="process_dbt_tables_dim",
        image=image_python,
        command=(
            "bash -c 'pip install dbt-bigquery && "
            "dbt run --model tag:stg tag:dim --profiles-dir /app/dbt_b3/ --project-dir /app/dbt_b3/ --target prd'"
        ),
    )

    process_dbt_table_fact = DockerOperator(
        task_id="process_dbt_table_fact",
        image=image_python,
        command=(
            "bash -c 'pip install dbt-bigquery && "
            "dbt run --model tag:fact --profiles-dir /app/dbt_b3/ --project-dir /app/dbt_b3/ --target prd'"
        ),
    )

    query_specific_actions = bigQueryOperator(
        task_name="specific_actions",
        query_dir="libs/queries/views/query_specific_actions.sql",
    )

    query_top10_preco_abertura = bigQueryOperator(
        task_name="top10_preco_abertura",
        query_dir="libs/queries/views/query_top10_preco_abertura.sql",
    )

    query_top5_mais_negociados = bigQueryOperator(
        task_name="top5_mais_negociados",
        query_dir="libs/queries/views/query_top5_mais_negociados.sql",
    )

    query_top5_var_neg = bigQueryOperator(
        task_name="top5_var_neg",
        query_dir="libs/queries/views/query_top5_var_neg.sql",
    )

    query_top5_var_pos = bigQueryOperator(
        task_name="top5_var_pos",
        query_dir="libs/queries/views/query_top5_var_pos.sql",
    )

    (
        extraction_data_api_from_raw_zone
        >> process_dbt_tables_dim
        >> process_dbt_table_fact
        >> [
            query_specific_actions,
            query_top10_preco_abertura,
            query_top5_mais_negociados,
            query_top5_var_neg,
            query_top5_var_pos,
        ]
    )

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

def readFile(file_path: str):
    fd = open(file_path)
    fileContent = fd.read()
    fd.close()
    return fileContent


def bigQueryOperator(task_name: str, query_dir: str):
    return BigQueryInsertJobOperator(
        task_id=task_name,
        gcp_conn_id="college_etl_project",
        configuration={
            "query": {
                "query": readFile(file_path=f"/home/vfdesouza/airflow/dags/{query_dir}"),
                "useLegacySql": False,
            }
        },
        location="US"
    )

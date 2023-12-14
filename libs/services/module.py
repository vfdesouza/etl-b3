import os
import requests
import zipfile
import re
import pandas as pd

from datetime import datetime, timedelta
from typing import Optional
from io import BytesIO
from time import sleep
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from google.oauth2 import service_account
from google.cloud.exceptions import ClientError
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.auth.exceptions import GoogleAuthError
from dotenv import load_dotenv

from libs.services.logger import Logger

load_dotenv()

BASE_URL_B3 = os.getenv("BASE_URL_B3")
BASE_URL_STATUS_INVEST = os.getenv("BASE_URL_STATUS_INVEST")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")


class Module:
    def __init__(self) -> None:
        pass

    def get_bigquery_client(self) -> bigquery.Client | None:
        """Returns an instance of the Google BigQuery client"""
        try:
            credentials = service_account.Credentials.from_service_account_file(
                GOOGLE_APPLICATION_CREDENTIALS,
                scopes=["https://www.googleapis.com/auth/cloud-platform"],
            )

            return bigquery.Client(
                credentials=credentials, project=credentials.project_id
            )

        except (GoogleAuthError, ClientError) as excep:
            Logger.warning(
                message=f"Error launching Google Bigquery client instance: {excep}"
            )

            return None

    def remove_file(self, directory: str) -> None:
        if os.path.exists(directory):
            os.remove(directory)
            Logger.warning(message=("------------------------------------------------"))

    def retry_request(self, url):
        try:
            retry_strategy = Retry(
                total=3,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=["GET"],
                backoff_factor=120,
            )

            adapter = HTTPAdapter(max_retries=retry_strategy)
            http = requests.Session()
            http.mount("https://", adapter)
            http.mount("http://", adapter)

            response = http.get(url)

        except Exception as e:
            Logger.error(message=f"ERROR: {e}")

        return response

    def get_statusinvest(self, company: str, base_url=BASE_URL_STATUS_INVEST):
        max_attempts = 10  # Número máximo de tentativas
        attempts = 0  # Inicializa o contador de tentativas

        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36"
        }

        url = base_url.format(company)
        lista_de_jsons = None

        while attempts < max_attempts:
            response = requests.get(url, headers=headers)

            if response.status_code == 200:
                lista_de_jsons = response.json()
                break
            else:
                Logger.error(
                    message=f"Tentativa {attempts + 1}: Falha na requisição. Código de status: {response.status_code}"
                )
                attempts += 1
                if attempts < max_attempts:
                    sleep(2)
                else:
                    raise Exception(
                        "Número máximo de tentativas atingido. A requisição não pôde ser concluída."
                    )
        return lista_de_jsons

    def check_day_week(self, type_format: str):
        day_week = str(datetime.now().strftime("%A"))

        day_mapping = {
            "Tuesday": 1,
            "Wednesday": 1,
            "Thursday": 1,
            "Friday": 1,
            "Saturday": 1,
            "Sunday": 2,
            "Monday": 3,
        }

        interval = day_mapping.get(day_week)

        check_format = {"date": "%Y-%m-%d", "date_br": "%d%m%Y"}

        date_format = check_format.get(type_format)

        return (datetime.now().date() - timedelta(days=interval)).strftime(
            format=date_format
        )

    def download_file_zip_from_txt(
        self, type_extraction: str, base_url=BASE_URL_B3, years: Optional[list] = None
    ) -> str | list:
        if type_extraction == "daily":
            extraction_date = self.check_day_week(type_format="date_br")

            url = f"{base_url}/COTAHIST_D{extraction_date}.ZIP"

            filename = f"COTAHIST_D{extraction_date}.TXT"

            response = self.retry_request(url)

            with zipfile.ZipFile(BytesIO(response.content), "r") as zipf:
                zipf.extractall(path=f"./data/")

            return f"./data/{filename}"

        elif type_extraction == "retroactive":
            list_directories = []
            for year in years:
                url = f"{base_url}/COTAHIST_A{year}.ZIP"
                directory = re.search(r"\d{4}", url).group()

                filename = url.split("/")[-1]

                response = self.retry_request(url)

            with zipfile.ZipFile(BytesIO(response.content), "r") as zipf:
                zipf.extractall(path=f"./data/{directory}")
            list_directories.append(
                [directory, f"./data/{directory}/{filename}".replace(".ZIP", ".TXT")]
            )

            return list_directories

    def load_txt_from_dataframe(
        self, path_to_file: str, list_companies: Optional[list] = None
    ) -> pd.DataFrame():
        columns_size = [
            2,
            8,
            2,
            12,
            3,
            12,
            10,
            3,
            4,
            13,
            13,
            13,
            13,
            13,
            13,
            13,
            5,
            18,
            18,
            13,
            1,
            8,
            7,
            13,
            12,
            3,
        ]
        colums_rename = [
            "tipo_registro",
            "data_pregao",
            "cod_bdi",
            "cod_negociacao",
            "tipo_mercado",
            "nome_empresa",
            "especificacao_papel",
            "prazo_dias_merc_termo",
            "moeda_referencia",
            "preco_abertura",
            "preco_maximo",
            "preco_minimo",
            "preco_medio",
            "preco_ultimo_negocio",
            "preco_melhor_oferta_compra",
            "preco_melhor_oferta_venda",
            "numero_negocios",
            "quantidade_papeis_negociados",
            "volume_total_negociado",
            "preco_exercicio",
            "indicador_correcao_precos",
            "data_vencimento",
            "fator_cotacao",
            "preco_exercicio_pontos",
            "codigo_isin",
            "num_distribuicao_papel",
        ]

        df_full = pd.read_fwf(filepath_or_buffer=path_to_file, widths=columns_size)

        df_full.columns = colums_rename

        adjust_values = [
            "preco_abertura",
            "preco_maximo",
            "preco_minimo",
            "preco_medio",
            "preco_ultimo_negocio",
            "preco_melhor_oferta_compra",
            "preco_melhor_oferta_venda",
            "volume_total_negociado",
            "preco_exercicio",
            "preco_exercicio_pontos",
        ]

        # df_filter_companies = df_full[df_full["cod_negociacao"].isin(list_companies)]

        df_full.loc[:, adjust_values] = df_full[adjust_values].apply(lambda x: x / 100)

        self.remove_file(directory=path_to_file)

        return df_full.astype(str)

    def load_table_from_dataframe_partitioning(
        self,
        dataset: str,
        table_name: str,
        column_partitioning: str,
        type_column_partitioning: str,
        partition_type: str,
        columns_clustering: list,
        dataframe: pd.DataFrame,
    ):
        """Loading partitioned data into BigQuery raw_zone"""

        bigquery_client = self.get_bigquery_client()

        if partition_type == "DAY":
            time_partitioning = bigquery.TimePartitioning(
                field=column_partitioning,
                type_=bigquery.TimePartitioningType.DAY,
                expiration_ms=None,
            )

        if partition_type == "YEAR":
            time_partitioning = bigquery.TimePartitioning(
                field=column_partitioning,
                type_=bigquery.TimePartitioningType.YEAR,
                expiration_ms=None,
            )

        clustering_fields = columns_clustering

        table_ref = bigquery_client.dataset(dataset).table(table_name)

        # schema = [
        #     bigquery.SchemaField(
        #         column, "DATETIME" if column == "extract_at" else "STRING"
        #     )
        #     for column in dataframe.columns
        # ]

        # Define the table definition with partitioning and clustering options

        table = bigquery.Table(table_ref)  # , schema=schema)
        table.time_partitioning = time_partitioning
        table.clustering_fields = clustering_fields

        try:
            bigquery_client.get_table(table_ref)
            Logger.warning(message=f"Table {table_name} already exists.")
        except NotFound:
            Logger.warning(message=f"Table {table_name} does not exist. Creating...")

        # parquet_options = bigquery.ParquetOptions()
        # parquet_options.enable_list_inference = False

        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField(column_partitioning, type_column_partitioning)
            ],
            write_disposition="WRITE_APPEND",
            ignore_unknown_values=True,
            time_partitioning=table.time_partitioning,
            clustering_fields=table.clustering_fields,
            # parquet_options=parquet_options,
        )

        job = bigquery_client.load_table_from_dataframe(
            dataframe, table_ref, job_config=job_config
        )

        # Wait for the job to complete
        job.result()

        # Print the number of rows inserted
        Logger.success(message=f"Loaded {job.output_rows} rows into {table.path}")

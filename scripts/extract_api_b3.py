import os
from datetime import datetime
from dotenv import load_dotenv

from libs.services.module import Module
from libs.services.logger import Logger

load_dotenv()

func = Module()

DATASET_RAW_ZONE_B3 = os.getenv("DATASET_RAW_ZONE_B3")


def main(years: list, companies: list) -> None:
    Logger.info(message="Extracting the B3 API .ZIP file.")

    list_path_to_files = func.download_file_zip_from_txt(years=years)

    Logger.info(message="Extraction completed.")

    for year, file in list_path_to_files:
        Logger.info(
            message=f"Creating the Dataframe of {year} data for companies: {companies}."
        )
        dataframe = func.load_txt_from_dataframe(file, companies)

        Logger.info(
            message=f"Dataframe loading completed for year {year}. Sending data to BigQuery in a partitioned and clustered table."
        )

        dataframe["data_pregao"] = dataframe["data_pregao"].apply(
            lambda x: datetime.strptime(str(x), "%Y%m%d").date()
        )

        func.load_table_from_dataframe_partitioning(
            dataset=DATASET_RAW_ZONE_B3,
            table_name=f"b3_{year}",
            column_partitioning="data_pregao",
            type_column_partitioning="DATE",
            partition_type="DAY",
            columns_clustering=["cod_negociacao"],
            dataframe=dataframe,
        )


if __name__ == "__main__":
    main(
        [2019, 2020, 2021, 2022, 2023],
        ["EMBR3", "PETR4", "NTCO3", "GGBR4", "CPLE6"],
    )

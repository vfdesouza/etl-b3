import os
from pandas import to_datetime, concat
from dotenv import load_dotenv

from libs.services.module import Module
from libs.services.logger import Logger

load_dotenv()

func = Module()

DATASET_RAW_ZONE_B3 = os.getenv("DATASET_RAW_ZONE_B3")
BASE_URL_B3 = os.getenv("BASE_URL_B3")


def main(years: list) -> None:
    Logger.info(message="Extracting the B3 API .ZIP file.")

    list_path_to_files = func.download_file_zip_from_txt(
        type_extraction="retroactive", years=years
    )

    Logger.info(message="Extraction completed.")

    df_list = []
    for year, file in list_path_to_files:
        Logger.info(message=f"Creating the Dataframe of {year} data for all companies.")
        dataframe = func.load_txt_from_dataframe(path_to_file=file)

        Logger.info(message=f"Dataframe loading completed for year {year}.")

        dataframe["data_pregao"] = to_datetime(
            dataframe["data_pregao"], format="%Y%m%d", errors="coerce"
        ).dt.date

        dataframe.dropna(subset=["data_pregao"], inplace=True)

        Logger.warning(
            message=f"Total data extracted for the year {year}: {len(dataframe)}"
        )

        df_list.append(dataframe)

    df_concat = concat(df_list)

    Logger.warning(
        message=f"Total data extracted for the years {[years]}: {len(df_concat)}"
    )

    func.load_table_from_dataframe_partitioning(
        dataset=DATASET_RAW_ZONE_B3,
        table_name=f"b3_retroactive",
        column_partitioning="data_pregao",
        type_column_partitioning="DATE",
        partition_type="DAY",
        columns_clustering=["cod_negociacao"],
        dataframe=df_concat,
    )


if __name__ == "__main__":
    main([2023])

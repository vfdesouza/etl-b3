import os
from pandas import to_datetime
from dotenv import load_dotenv

from libs.services.module import Module
from libs.services.logger import Logger

load_dotenv()

func = Module()

DATASET_RAW_ZONE_B3 = os.getenv("DATASET_RAW_ZONE_B3")


def main() -> None:
    Logger.info(message="Extracting the B3 API .ZIP file.")

    file = func.download_file_zip_from_txt(type_extraction="daily")

    Logger.info(message="Extraction completed.")

    Logger.info(message="Creating the Dataframe data for all companies.")
    dataframe = func.load_txt_from_dataframe(path_to_file=file)
    
    print(dataframe.head())

    Logger.info(message="Dataframe loading completed.")

    dataframe["data_pregao"] = to_datetime(
        dataframe["data_pregao"], format="%Y%m%d", errors="coerce"
    ).dt.date

    dataframe.dropna(subset=["data_pregao"], inplace=True)

    func.load_table_from_dataframe_partitioning(
        dataset=DATASET_RAW_ZONE_B3,
        table_name="b3_retroactive",
        column_partitioning="data_pregao",
        type_column_partitioning="DATE",
        partition_type="DAY",
        columns_clustering=["cod_negociacao"],
        dataframe=dataframe,
    )


if __name__ == "__main__":
    main()

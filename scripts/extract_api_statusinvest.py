import os
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime

from libs.services.module import Module
from libs.services.logger import Logger

load_dotenv()

func = Module()

DATASET_RAW_ZONE_STATUS_INVEST = os.getenv("DATASET_RAW_ZONE_STATUS_INVEST")


def main(companies: list) -> None:
    Logger.info(
        message=f"Extracting the data and creating the dataframe for companies: {companies}."
    )

    df_list = []
    for company in companies:
        list_json_company = func.get_statusinvest(company=company)
        df_company = pd.DataFrame(list_json_company).astype(str)
        df_company["company"] = company
        df_company["year"] = df_company["year"].apply(
            lambda x: datetime.strptime(str(x), "%Y").date()
        )

        df_list.append(df_company)

    concatenated_df = pd.concat(df_list)

    Logger.info(
        message=f"Dataframe loading completed. Sending data to BigQuery in a partitioned and clustered table."
    )

    func.load_table_from_dataframe_partitioning(
        dataset=DATASET_RAW_ZONE_STATUS_INVEST,
        table_name=f"status_invest",
        column_partitioning="year",
        type_column_partitioning="DATE",
        partition_type="YEAR",
        columns_clustering=["year", "quarter"],
        dataframe=concatenated_df,
    )


if __name__ == "__main__":
    main(["EMBR3", "PETR4", "NTCO3", "GGBR4", "CPLE6"])

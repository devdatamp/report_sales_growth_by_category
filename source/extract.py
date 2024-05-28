import pandas as pd
import numpy as np
import datetime
import warnings

warnings.filterwarnings('ignore')


def extract_data_sales_offline(year: str, *months):
    # Data Existing
    df_offline = pd.DataFrame()

    for month in months:
        data_offline = pd.read_parquet(
            f"s3://mega-dev-lake/ProcessedData/Sales/sales_detail_indie/{year}/{month}/data.parquet"
        )

        data_offline = data_offline[
            (data_offline["status_order"] != 7)
            & (data_offline["issettled"] != False)
            & (data_offline["istransaction"] != "0")
            & (data_offline["channel"] != "ONLINE")
        ]

        df_offline = pd.concat([df_offline, data_offline])

    df_offline["order_create_date"] = pd.to_datetime(
        df_offline["order_create_date"]
    ).dt.strftime("%Y-%m-%d")
    return df_offline


def extract_data_sales_online(year: str, *months):
    df_online = pd.DataFrame()

    for month in months:
        data_online = pd.read_parquet(
            f"s3://mega-dev-lake/ProcessedData/Sales/sales_online/{year}/{month}/data.parquet"
        )

        df_online = pd.concat([df_online, data_online])

    df_online.columns = df_online.columns.str.lower()
    df_online.columns = df_online.columns.str.replace(" ", "_")
    df_online = df_online[df_online["status_sales"] != "CANCELED"]
    df_online["date"] = pd.to_datetime(df_online["date"]).dt.strftime("%Y-%m-%d")

    df_online["marketplace"] = np.where(
        df_online["marketplace"].isin(
            [
                "MANZONESTORE.ID",
                "MINIMALSTORE.ID",
                "MOCSTORE.ID",
                "WEBSITE",
            ]
        ),
        "SHOPIFY",
        df_online["marketplace"],
    )
    
    return df_online


def extract_pattern_data(pattern_path, sheet_name, *months):
    # Data Pattern
    pattern = pd.read_excel(pattern_path, sheet_name=sheet_name)
    list_month = [month for month in months]

    pattern = pattern[
        pd.to_datetime(pattern["pattern_date"]).dt.strftime("%m").isin(list_month)
    ]
    pattern["pattern_date"] = pd.to_datetime(pattern["pattern_date"]).dt.strftime(
        "%Y-%m-%d"
    )
    pattern["pattern_last_year"] = pd.to_datetime(
        pattern["pattern_last_year"]
    ).dt.strftime("%Y-%m-%d")
    return pattern


def extract_master_store(*months: str):
    master = pd.read_parquet(
        "s3://mega-dev-lake/Staging/Master/Master Store/2024/data.parquet"
    )

    month_name = [pd.to_datetime(month, format="%m").month_name() for month in months]

    if month_name != "All":
        master = master[master["month"].isin(month_name)]
    else:
        pass

    return master


def extract_master_online(master):
    master_online = master[master["main_channel"] == "ONLINE"]
    master_online["marketplace"] = master_online["stdname"].str.split(" ").str.get(2)
    master_online["brand"] = master_online["stdname"].str.split(" ").str.get(1)
    master_online["marketplace"] = master_online["marketplace"].fillna("")

    master_online["marketplace"] = np.where(
        master_online["marketplace"].isin(
            [
                "MANZONESTORE.ID",
                "MINIMALSTORE.ID",
                "MOCSTORE.ID",
                "WEBSITE",
            ]
        ),
        "SHOPIFY",
        master_online["marketplace"],
    )

    master_online = master_online[master_online["openstatus"] == "OPEN"]
    return master_online

def main_extract(year, *months):
    pattern_path = "s3://mega-lake/ProcessedData/Sales/pattern/pattern_24_with_festive.xlsx"
    
    sheet_name = "offline"

    # Extract existing data
    df_offline = extract_data_sales_offline(year, *months)
    df_online = extract_data_sales_online(year, *months)
    pattern = extract_pattern_data(pattern_path, sheet_name, *months)
    master_store = extract_master_store(*months)
    master_online = extract_master_online(master_store)
    
    # Extract historical data (last year)
    list_month_lastyear = pd.to_datetime(pattern[~(pattern['pattern_last_year'].isna())]['pattern_last_year']).dt.strftime("%m").unique()
    year_hist = pd.to_datetime(pattern[~(pattern['pattern_last_year'].isna())]['pattern_last_year']).dt.strftime("%Y").unique()[0]
    
    df_offline_hist = extract_data_sales_offline(year_hist, *list_month_lastyear)
    df_online_hist = extract_data_sales_online(year_hist, *list_month_lastyear)
    
    return df_offline, df_online, pattern, master_store, master_online, df_offline_hist, df_online_hist
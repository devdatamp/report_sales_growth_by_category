import pandas as pd
import numpy as np
import datetime
import warnings

warnings.filterwarnings('ignore')

def transformasi_sales(df_offline, df_online, master_online):
    df_offline_agg = (
        df_offline.groupby(
            ["dataareaid", "axcode", "order_create_date", "category", "subcategory"],
            as_index=False,
        )
        .agg({"cust_paid_peritem": "sum", "quantity": "sum", "hpp_total": "sum"})
        .rename(columns={"order_create_date": "date"})
    )

    df_online_agg = (
        master_online[["dataareaid", "axcode", "brand", "marketplace"]]
        .merge(
            df_online.groupby(
                ["brand", "marketplace", "date", "category", "subcategory"],
                as_index=False,
            )
            .agg(
                {"value_after_voucher": "sum", "qty_sold": "sum", "existing_hpp": "sum"}
            )
            .rename(
                columns={
                    "value_after_voucher": "cust_paid_peritem",
                    "qty_sold": "quantity",
                    "existing_hpp": "hpp_total",
                }
            ),
            "right",
            on=["brand", "marketplace"],
        )
        .drop(columns=["brand", "marketplace"])
    )

    df_agg = pd.concat([df_offline_agg, df_online_agg])
    return df_agg


def transformasi_with_pattern_exsiting(df_agg, pattern):
    df_agg_pattern = df_agg.merge(
        pattern[["pattern_date", "pattern_last_year"]],
        "left",
        left_on="date",
        right_on="pattern_date",
    )
    return df_agg_pattern


def transformasi_with_pattern_hist(df_agg, pattern):
    df_agg_pattern = df_agg.merge(
        pattern[["pattern_date", "pattern_last_year"]],
        "left",
        left_on="date",
        right_on="pattern_last_year",
    )
    return df_agg_pattern


def transformasi_growth_daily(df_existing, df_hist):
    # Transformasi Growth
    df_hist["date"] = df_hist["pattern_date"]
    df_growth = df_existing.merge(
        df_hist,
        "outer",
        on=[
            "dataareaid",
            "axcode",
            "category",
            "subcategory",
            "date",
            "pattern_last_year",
        ],
        suffixes=("_this_year", "_last_year"),
        indicator=True,
    ).reset_index(drop=True)
    df_growth[df_growth.select_dtypes(include="number").columns] = df_growth[
        df_growth.select_dtypes(include="number").columns
    ].fillna(0)

    return df_growth


def transformasi_to_report(df_growth, master_store):
    df_report = master_store[
        [
            "dataareaid",
            "channel",
            "brand",
            "axcode",
            "stdname",
            "so dept head",
            "area head",
            "city head",
        ]
    ].merge(df_growth, "right", on=["axcode", "dataareaid"]).drop(columns=['pattern_date_this_year', 'pattern_date_last_year', '_merge'])


    df_report.columns = df_report.columns.str.replace("_", " ").str.title()
    return df_report

def main_transformasi(df_offline, df_online, pattern, df_offline_hist, df_online_hist, master_online, master_store):
    df_sales_existing = transformasi_sales(df_offline, df_online, master_online)
    df_sales_hist = transformasi_sales(df_offline_hist, df_online_hist, master_online)
    
    df_sales_pattern_existing = transformasi_with_pattern_exsiting(df_sales_existing, pattern)
    df_sales_pattern_hist = transformasi_with_pattern_hist(df_sales_hist, pattern)

    max_date = df_sales_pattern_existing['date'].max()
    df_sales_pattern_hist = df_sales_pattern_hist[df_sales_pattern_hist['pattern_date'] <= max_date]

    df_growth = transformasi_growth_daily(df_sales_pattern_existing, df_sales_pattern_hist).query("_merge == 'both'")
    df_report = transformasi_to_report(df_growth,master_store)
    
    return df_report
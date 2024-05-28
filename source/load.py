import pandas as pd
import numpy as np
import datetime
import warnings


def load(df, *months):
    file_name = "data.xlsx"
    for month in months:
        path = f"s3://mega-dev-lake/Staging/Sales/akumulasi/growth_peritem_v2/{month}/{file_name}"
        df.to_excel(path, index=False)

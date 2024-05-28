import pandas as pd
import numpy as np
import datetime
import warnings

import sys

sys.path.append("source")
from extract import *
from transform import *
from load import *

warnings.filterwarnings("ignore")


def main(year, *months):
    print("--- Start Extract ---")
    (
        df_offline,
        df_online,
        pattern,
        master_store,
        master_online,
        df_offline_hist,
        df_online_hist,
    ) = main_extract(year, *months)

    print("--- Start Transformasi ---")
    df_report = main_transformasi(
        df_offline,
        df_online,
        pattern,
        df_offline_hist,
        df_online_hist,
        master_online,
        master_store,
    )

    print("--- Start Load Data ---")
    load(df_report, *months)
    print("--- Data Loaded ---")


if __name__ == "__main__":
    year = input("Enter year: ")
    month = input("Enter month: ")
    main(year, month)

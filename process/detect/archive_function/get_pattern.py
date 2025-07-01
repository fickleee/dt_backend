import pandas as pd

from .common import ZERO_HOUR, ZERO_RATE, ZERO_THRESHOLD, DOUBLE_RATE
from .common import UMAP_PARAMS
from .common import get_i_col
from .fetch_data import get_station_data, get_env_data
from process.detect.archive_function.constants import PROC_TIME_WINDOW

def is_zero_i(i_data: pd.DataFrame, string_id: str, days: int) ->bool:
    i_col = string_id
    # i_data = i_data.iloc[:, [0, string_id+1]]
    i_data = i_data[[i_col]].copy()
    i_data['flag'] = i_data[i_col].apply(lambda x: 0 if x<ZERO_THRESHOLD else 1)
    cnt = i_data["flag"].sum()

    # hour per day and rate
    return cnt < ZERO_RATE * (ZERO_HOUR*days)


def is_double_i(i_data: pd.DataFrame, string_id: str) ->bool:
    # before using the i_data is filtered by hour
    i_data = i_data.sample(frac=DOUBLE_RATE, random_state=42)
    i_col = string_id
    sum1 = i_data[i_col].sum()
    
    for col in i_data.columns:
        if col in ["time", i_col]:
            continue
        # escape 0 divide
        sum2 = i_data[col].sum() + 1e-5
        factor = round(sum1/sum2, 0)
        if factor==2 and i_data[i_col].iloc[0]*1.5 > i_data.iloc[0, 1:].max():
            return True

    return False


def perform_dim_reduction():
    station_data = get_station_data()
    env_data = get_env_data()
    days = PROC_TIME_WINDOW

    box_groups = {}

    for key, inv_data in station_data.items():
        inv_data = inv_data.copy()
        box_id, inv_id = key.split("-")
        if box_id not in box_groups:
            box_groups[box_id] = dict()
        box_groups[box_id][key] = inv_data
        inv_data["time"] = pd.to_datetime(inv_data["time"])
        inv_data.set_index("time", inplace=True)    

        cols = inv_data.columns

        for col in cols:
            if is_zero_i(inv_data, col, days) or is_double_i(inv_data, col):
                continue

            arr_data = inv_data[[col]]


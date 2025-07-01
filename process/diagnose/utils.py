import pandas as pd
from .common import ZERO_THRESHOLD, ZERO_RATE, ZERO_HOUR, DOUBLE_RATE
def is_zero_i(i_data: pd.DataFrame, string_id: str, days: int) ->bool:
    i_col = string_id
    i_data = i_data[[i_col]].copy()
    i_data['flag'] = i_data[i_col].apply(lambda x: 0 if x<ZERO_THRESHOLD else 1)
    cnt = i_data["flag"].sum()

    # hour per day and rate
    return cnt < ZERO_RATE * (ZERO_HOUR*days)


# 用于检测光伏组串是否存在"电流翻倍"异常情况，即某个组串的电流值可能是其他组串的两倍
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
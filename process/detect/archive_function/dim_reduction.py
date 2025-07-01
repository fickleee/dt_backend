import pandas as pd
from umap import UMAP
import os
from .save_to_result import construct_result_template,update_identifier,update_rdc_positions

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
SAVE_DIR = "./data"

TIME_WINDOW = 30
# TIME_WINDOW = 3 # 测试用
START_HOUR = "10:00"
END_HOUR = "14:00"

ZERO_THRESHOLD = 0.1
ZERO_HOUR = 5
ZERO_RATE = 0.1

DOUBLE_RATE = 0.2

UMAP_PARAMS = {
    "n_components": 2,
    "n_neighbors": 15,
    "n_jobs": -1,
}

# 判断光伏组串（PV string）在指定天数内的电流数据是否可以被视为"零电流"状态
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

def perform_dim_reduction(station_data: dict, env_data: pd.DataFrame, station_name, end_date, repo_abs_path,time_window) ->list[pd.DataFrame]:
    """
    执行降维计算并构造结果模板
    Returns:
        list[pd.DataFrame]: 降维结果列表
    """
    construct_result_template(station_data, end_date, station_name, repo_abs_path)
    error_types = []

    dr_inv_list, rdc_positions = perform_dim_reduction_calc(station_data, env_data, error_types, end_date,time_window)

    save_dir = os.path.join(repo_abs_path,'data', station_name, "results")
    file_path = os.path.join(save_dir, f"{end_date}.json")

    update_identifier(file_path, error_types)
    update_rdc_positions(file_path, rdc_positions)
    
    return dr_inv_list

def perform_dim_reduction_calc(station_data: dict, env_data: pd.DataFrame, error_types: list, end_date, time_window) ->list[pd.DataFrame]:
    rdc_positions = {} 
    start_h = START_HOUR
    end_h = END_HOUR
    days = time_window
    # skip_box_list = ["002", "003", "004", "005", "006", "007", "008", "011", "015", "038"]
    # SELECT_BOX = ["012","013"]

    wt_data = env_data.between_time(start_h, end_h)
    box_groups = {}

    for key, inv_data in station_data.items():
        box_id, inv_id = key.split("-")
        # if box_id not in SELECT_BOX:
        #     continue
        if box_id not in box_groups:
            box_groups[box_id] = dict()
        if inv_id not in box_groups[box_id]:
            box_groups[box_id][inv_id] = dict()

        inv_data = inv_data.copy()
        inv_data["time"] = pd.to_datetime(inv_data["time"])
        inv_data.set_index("time", inplace=True)
        cols = inv_data.columns

        for col in cols:
            string_id = col.split('输入电流')[0].replace('PV', '').zfill(3)
            error_info = {
                'box_id': box_id,
                'inverter_id': inv_id,
                'string_id': string_id,
                'error_type': None
            }
            
            if is_zero_i(inv_data, col, days):
                error_info['error_type'] = 'zero_current'
                error_types.append(error_info)
                continue
            elif is_double_i(inv_data, col):
                error_info['error_type'] = 'double_current'
                error_types.append(error_info)
                continue
            else:
                error_types.append(error_info)  # 记录正常状态

            arr_data = inv_data[[col]].between_time(start_h, end_h)
            arr_df = arr_data.join(wt_data)
            arr_df["i"] = ((arr_df[col]/arr_df["rad"].replace(0, float('nan')))).fillna(0)

            dr_df = pd.DataFrame()
            for t, g in arr_df["i"].groupby(arr_df["i"].index.date):
                tmp_df = g.to_frame().transpose()
                tmp_df.index = pd.DatetimeIndex([t])
                tmp_df.columns = [h for h in g.index.hour]
                dr_df = pd.concat([dr_df, tmp_df], axis=0)

            box_groups[box_id][inv_id][string_id] = dr_df

    dr_out_data = dict()

    for box_id, box_data in box_groups.items():
        if len(box_data) == 0:
            continue 

        dr_df_list = []
        dr_id_list = []
        for inv_id, inv_data in box_data.items():
            for string_id, dr_df in inv_data.items():
                dr_df_list.append(dr_df)
                dr_id_list.append(f"{box_id}-{inv_id}-{string_id}")
        box_dr_df = pd.concat(dr_df_list, axis=0)

        if box_dr_df.isnull().values.any():
            print("数据包含 NaN 值，需要进行处理。")
            box_dr_df = box_dr_df.fillna(0)
        
        dr_model = UMAP(**UMAP_PARAMS)
        dr_data = dr_model.fit_transform(box_dr_df)

        for i in range(int(len(dr_data)/days)):
            tmp_df = pd.DataFrame(dr_data[i*days:(i+1)*days], columns=["x", "y"])
            device_id = dr_id_list[i]
            dr_out_data[device_id] = tmp_df

            rdc_positions[device_id] = {
                'x': tmp_df['x'].tolist(),
                'y': tmp_df['y'].tolist()
            }

    end_date = pd.to_datetime(end_date) 
    start_date = end_date - pd.DateOffset(days=time_window-1) 
    time_index = pd.date_range(start=start_date, end=end_date, freq='D')
    dr_inv_dict = dict()

    for k in dr_out_data:
        dr_df = pd.DataFrame(dr_out_data[k])
        dr_df.index = time_index
        dr_df["label"] = 1
        dr_df["id"] = f"{k}@" + dr_df.index.strftime('%Y-%m-%d')
        key = "-".join(k.split("-")[:-1])

        if key in dr_inv_dict:
            dr_inv_dict[key].append(dr_df)
        else:
            dr_inv_dict[key] = [dr_df]
        
    dr_inv_list = [pd.concat(inv, axis=0) for inv in dr_inv_dict.values()]

    return dr_inv_list, rdc_positions

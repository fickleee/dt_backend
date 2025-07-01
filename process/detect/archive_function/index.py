# 使用逻辑
import os
import json
from process.detect.archive_function.fetch_data import get_station_data, get_env_data
from process.detect.archive_function.dim_reduction import perform_dim_reduction
from process.detect.archive_function.anomaly_calc import calc_anomaly_score, calc_deg_score
from process.detect.utils import get_time_range

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
SAVE_DIR = "./data"
TIME_WINDOW = 30

# def perform_detection_process(station_name, end_date):
#     """
#     执行光伏组串异常检测和劣化分析
    
#     Args:
#         save_dir: 数据缓存目录，默认为 "data_cache"
        
#     Returns:
#         bool: 处理成功返回 True，失败返回 False
#     """
#     save_dir = os.path.join(SAVE_DIR, station_name, "results")
#     result_path = os.path.join(save_dir, f"{end_date}.json")
#     if os.path.exists(result_path):
#         print("数据已存在，无需重复处理")
#         return 200
    
#     print("场站名称：", station_name)
#     print("截止日期：", end_date)
    
#     print("开始读取电气量数据")
#     station_data = get_station_data(station_name=station_name, end_date=end_date)
#     print("电气量数据读取成功")
#     env_data = get_env_data(station_name=station_name, end_date=end_date)
#     # 执行降维计算
#     print("开始读取辐照数据")
#     dr_inv_list = perform_dim_reduction(station_data, env_data, station_name, end_date)
#     print("辐照数据读取成功")
#     # 计算异常值
#     print("开始计算组串异常劣化情况")
#     ano_st_df = calc_anomaly_score(dr_inv_list,station_name,end_date)
#     # 计算劣化率
#     deg_st_df = calc_deg_score(dr_inv_list, ano_st_df, station_name, end_date)
#     print("组串异常劣化情况计算完成")

#     return 200


def detect_schedule(station_name, end_date, repo_abs_path, time_window=30):
    """
    执行光伏组串异常检测和劣化分析
    
    Args:
        save_dir: 数据缓存目录，默认为 "data_cache"
        
    Returns:
        bool: 处理成功返回 True，失败返回 False
    """
    # save_dir = os.path.join(SAVE_DIR, station_name, "results")
    save_dir = os.path.join(repo_abs_path,'data', station_name, "results")
    result_path = os.path.join(save_dir, f"{end_date}.json")
    # if os.path.exists(result_path):
    #     print("数据已存在，无需重复处理")
    #     return 200
    
    print("场站名称：", station_name)
    print("截止日期：", end_date)

    start_window_timestamp,end_window_timestamp = get_time_range(end_date, previous_day=time_window-1)
    
    print("开始读取电气量数据")
    station_data = get_station_data(station_name=station_name,repo_abs_path=repo_abs_path,start_window_timestamp=start_window_timestamp,end_window_timestamp=end_window_timestamp)
    print("电气量数据读取成功")
    env_data = get_env_data(station_name=station_name, repo_abs_path=repo_abs_path, start_window_timestamp=start_window_timestamp,end_window_timestamp=end_window_timestamp)
    # 执行降维计算
    print("开始读取辐照数据")
    dr_inv_list = perform_dim_reduction(station_data, env_data, station_name, end_date, repo_abs_path,time_window)
    print("辐照数据读取成功")
    # 计算异常值
    print("开始计算组串异常劣化情况")
    ano_st_df = calc_anomaly_score(dr_inv_list,station_name,end_date, repo_abs_path)
    # 计算劣化率
    deg_st_df = calc_deg_score(dr_inv_list, ano_st_df, station_name, end_date, repo_abs_path)
    print("组串异常劣化情况计算完成")

    return 200

if __name__ == '__main__':
    station_name = 'datu'
    end_date = '2024-08-01'
    repo_abs_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    time_window=30
    detect_schedule(station_name=station_name, end_date=end_date, repo_abs_path=repo_abs_path, time_window=time_window)
        

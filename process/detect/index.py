import os
import json
from process.detect.utils import get_anomalous_string_ids, get_history_timestamp, update_degradation_scores, update_degradation_scores_dict
from process.detect.degradation import compute_degradation_score
from process.detect.data_reader import get_current_rad_df, get_current_rad_df_orm
import logging
import random

def random_01_03():
    return round(random.uniform(0.1, 0.3), 4)

# 日志配置（只需在模块顶部配置一次即可）
logger = logging.getLogger(__name__)
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
SAVE_DIR = "./data"
TIME_WINDOW = 30

def detect_schedule(station_name, end_date, repo_abs_path, time_window=30):
    print("场站名称：", station_name)
    print("截止日期：", end_date)

    save_dir = os.path.join(repo_abs_path,'data', station_name, "results")
    result_path = os.path.join(save_dir, f"{end_date}.json")

    anomalous_ids = get_anomalous_string_ids(result_path)
    print("异常设备ID列表：", anomalous_ids)

    history_timestamp_tuple = get_history_timestamp(end_date, time_window=time_window)

    # print(anomalous_ids)
    current_df, rad_df = get_current_rad_df(repo_abs_path,station_name, history_timestamp_tuple, anomalous_ids)

    for string_id in anomalous_ids:
        degradation_score = compute_degradation_score(string_id, end_date, time_window, current_df, rad_df)
        update_degradation_scores(string_id, degradation_score,result_path)
    
    print("低效劣化识别完成")
    return 200

def detect_schedule_orm(station_name, end_date, repo_abs_path, time_window=30, database_manager=None, station_model=None):
    logger.info(f"{station_name}_detect started at {end_date}")

    save_dir = os.path.join(repo_abs_path,'data', station_name, "results")
    result_path = os.path.join(save_dir, f"{end_date}.json")

    logger.info(f"\t{station_name}_step1 start: get anomalous string ids")
    anomalous_ids = get_anomalous_string_ids(result_path)

    logger.info(f"\t{station_name}_step2 start: get history timestamp")
    # history_timestamp_tuple = get_history_timestamp(end_date, time_window=time_window)

    logger.info(f"\t{station_name}_step3 start: get current and rad data")
    # current_df, rad_df = get_current_rad_df_orm(station_name, history_timestamp_tuple, anomalous_ids, database_manager=database_manager, station_model=station_model)

    logger.info(f"\t{station_name}_step4 start: compute degradation scores")

    degradation_dict = dict()

    for string_id in anomalous_ids:
        # degradation_score = compute_degradation_score(string_id, end_date, time_window, current_df, rad_df)
        degradation_score = random_01_03()  # 模拟计算劣化分数
        degradation_dict[string_id] = degradation_score


    update_degradation_scores_dict(degradation_dict, result_path)

    logger.info(f"{station_name}_detect completed at {end_date}")
    return 200

if __name__ == '__main__':
    station_name = 'datu'
    end_date = '2025-03-31'
    end_date = '2025-03-31'
    repo_abs_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    time_window=30
    detect_schedule(station_name=station_name, end_date=end_date, repo_abs_path=repo_abs_path)
    detect_schedule(station_name=station_name, end_date=end_date, repo_abs_path=repo_abs_path)
        

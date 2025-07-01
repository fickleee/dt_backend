from datetime import datetime
from datetime import datetime
import os
from process.diagnose.data_reader import read_data, read_data_orm
from process.diagnose.data_transformer import trans_data_byStation, detect_anomalies_byStation
from process.diagnose.model_predictor import model_byStation
from process.diagnose.result_saver import save_results, save_anomaly_identifiers, save_history_intensity
import logging

# 日志配置（只需在模块顶部配置一次即可）
logger = logging.getLogger(__name__)

def diagnosis_schedule(process_date, station_name, repo_abs_path):
    print("开始diagnosis的process部分")
    update_time = datetime.strptime(process_date, "%Y-%m-%d")
    database_path = os.path.join(repo_abs_path, 'database', f'{station_name}.db')
    
    # 1. 数据读取
    data, is_30_days = read_data(station_name, update_time, database_path)
    # data, is_30_days = read_data_orm(station_name, update_time, database_manager, station_model)
    if not is_30_days:
        print("警告：数据库中当前的历史数据不足30天，诊断无法进行！！！")
        return
    print("完成数据读取")
    print(f"数据读取完成，共 {len(data)} 条记录")
    print(f"数据读取完成，共 {len(data)} 条记录")

    if data:
        save_history_intensity(data, station_name, update_time, repo_abs_path)
        print("历史电流数据保存完成")
        # 2. 异常检测
        anomaly_identifiers = detect_anomalies_byStation(data)
        print("异常检测完成")
        
        # 3. 保存异常标识结果
        save_anomaly_identifiers(anomaly_identifiers, station_name, update_time, repo_abs_path)
        print("异常标识保存完成")
        
        # 4. 数据转换
        trans_data = trans_data_byStation(data, anomaly_identifiers)
        save_history_intensity(data, station_name, update_time, repo_abs_path)
        print("历史电流数据保存完成")
        # 2. 异常检测
        anomaly_identifiers = detect_anomalies_byStation(data)
        print("异常检测完成")
        
        # 3. 保存异常标识结果
        save_anomaly_identifiers(anomaly_identifiers, station_name, update_time, repo_abs_path)
        print("异常标识保存完成")
        
        # 4. 数据转换
        trans_data = trans_data_byStation(data, anomaly_identifiers)
        print("数据转换完成")
        
        # 5. 模型预测
        model_result = model_byStation(trans_data,repo_abs_path)
        
        # 5. 模型预测
        model_result = model_byStation(trans_data,repo_abs_path)
        print("模型预测完成")
        
        # 6. 保存结果
        save_results(model_result, station_name, update_time, repo_abs_path)
        
        # 6. 保存结果
        save_results(model_result, station_name, update_time, repo_abs_path)
        print("诊断结果保存完成")
    else:
        print("data is none")

def diagnosis_schedule_orm(process_date, station_name, repo_abs_path, database_manager, station_model):
    logger.info(f"{station_name}_diagnosis started at {process_date}")
    update_time = datetime.strptime(process_date, "%Y-%m-%d")
    database_path = os.path.join(repo_abs_path, 'database', f'{station_name}.db')
    
    # 1. 数据读取
    logger.info(f"\t{station_name}_step1 start: read data")
    data, is_30_days = read_data_orm(station_name, update_time, database_manager, station_model)
    if not is_30_days:
        logger.warning(f"\t{station_name}_step1 warning: insufficient historical data")
        return
    if data:
        logger.info(f"\t{station_name}_step1 completed: read {len(data)} records")

        save_history_intensity(data, station_name, update_time, repo_abs_path)
        # 2. 异常检测
        logger.info(f"\t{station_name}_step2 start: detect anomalies")
        anomaly_identifiers = detect_anomalies_byStation(data)

        # 3. 保存异常标识结果
        logger.info(f"\t{station_name}_step3 start: save anomaly identifiers")
        save_anomaly_identifiers(anomaly_identifiers, station_name, update_time, repo_abs_path)

        # 4. 数据转换
        logger.info(f"\t{station_name}_step4 start: transform data")
        trans_data = trans_data_byStation(data, anomaly_identifiers)

        # 5. 模型预测
        logger.info(f"\t{station_name}_step5 start: model prediction")
        model_result = model_byStation(trans_data,repo_abs_path)

        # 6. 保存结果
        logger.info(f"\t{station_name}_step6 start: save results")
        save_results(model_result, station_name, update_time, repo_abs_path)

        logger.info(f"{station_name}_diagnosis completed at {process_date}")
    else:
        logger.warning(f"\t{station_name}_step1 warning: no data available, skipping diagnosis")


if __name__ == '__main__':
    station_name = 'eryuan'
    repo_abs_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    process_date = '2025-06-01'

    diagnosis_schedule(process_date, station_name, repo_abs_path)
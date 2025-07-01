import os
import json
import pandas as pd
from datetime import datetime, timedelta
from collections import defaultdict
import logging

# 日志配置（只需在模块顶部配置一次即可）
logger = logging.getLogger(__name__)

def save_results(model_result, station_name, update_time, repo_abs_path):
    """
    保存模型预测结果
    
    Args:
        model_result: 模型预测结果
        station_name: 电站名称
        update_time: 更新时间
        repo_abs_path: 项目根路径
    """
    date_str = update_time.strftime('%Y-%m-%d') 
    results_folder = os.path.join(repo_abs_path, 'data', station_name, 'results')
    
    # 确保目录存在
    os.makedirs(results_folder, exist_ok=True)
    
    json_file_path = os.path.join(results_folder, f'{date_str}.json')

    if os.path.exists(json_file_path):
        with open(json_file_path, 'r', encoding='utf-8') as json_file:
            try:
                data = json.load(json_file)
                # for key in data['results']:
                #     data['results'][key]['diagnosis_results'] = ['0']
                for key, model_info in model_result.items():
                    existing_obj = data['results'].get(key)
                    if existing_obj:
                        existing_obj['diagnosis_results'] = model_info['diagnosis_results']
                    else:
                        data['results'][key] = model_info
                with open(json_file_path, 'w', encoding='utf-8') as json_file:
                    json.dump(data, json_file, ensure_ascii=False)
            except json.JSONDecodeError:
                logger.error('Error decoding JSON from the file.')
    else:
        # 创建新的 JSON 文件
        new_data = {
            'date': date_str,
            'results': model_result  
        }
        with open(json_file_path, 'w', encoding='utf-8') as json_file:
            json.dump(new_data, json_file, ensure_ascii=False)

def save_anomaly_identifiers(anomaly_identifiers, station_name, update_time, repo_abs_path):
    """
    保存异常标识结果到JSON文件，与diagnosis_results保存在同一个文件中
    
    Args:
        anomaly_identifiers: 异常标识字典
        station_name: 电站名称
        update_time: 更新时间
        repo_abs_path: 项目根路径
    """
    date_str = update_time.strftime('%Y-%m-%d')
    results_folder = os.path.join(repo_abs_path, 'data', station_name, 'results')
    
    # 确保目录存在
    os.makedirs(results_folder, exist_ok=True)
    
    json_file_path = os.path.join(results_folder, f'{date_str}.json')
    logger.info(f"save anomaly identifiers to: {json_file_path}")

    # 重新组织anomaly_identifiers的格式，使其与diagnosis_results的结构一致
    formatted_anomaly_data = {}
    for unique_key, anomaly_type in anomaly_identifiers.items():
        formatted_anomaly_data[unique_key] = {
            'anomaly_identifier': anomaly_type
        }
    
    if os.path.exists(json_file_path):
        # 文件存在，读取并更新
        with open(json_file_path, 'r', encoding='utf-8') as json_file:
            try:
                data = json.load(json_file)
                
                # 确保results字段存在
                if 'results' not in data:
                    logger.info("results not found, creating new results dictionary")
                    data['results'] = {}
                
                # 为每个组串添加anomaly_identifier字段
                for key, anomaly_info in formatted_anomaly_data.items():
                    if key in data['results']:
                        # print(f"正在更新组串 {key} 的anomaly_identifier: {anomaly_info['anomaly_identifier']}")
                        # 如果组串已存在，添加anomaly_identifier字段
                        data['results'][key]['anomaly_identifier'] = anomaly_info['anomaly_identifier']
                    else:
                        # 如果组串不存在，创建新条目
                        logger.info(f"Creating new entry for string {key}")
                        data['results'][key] = anomaly_info
                
                # 写回文件
                with open(json_file_path, 'w', encoding='utf-8') as json_file:
                    json.dump(data, json_file, ensure_ascii=False)

                logger.info(f"Anomaly identifiers have been updated to the existing file: {json_file_path}")

            except json.JSONDecodeError:
                logger.error('Error decoding JSON from the file.')
    else:
        # 文件不存在，创建新文件
        new_data = {
            'date': date_str,
            'results': formatted_anomaly_data
        }
        
        with open(json_file_path, 'w', encoding='utf-8') as json_file:
            json.dump(new_data, json_file, ensure_ascii=False)

        logger.info(f"Anomaly identifiers have been saved to the new file: {json_file_path}")

    # 统计信息
    total_strings = len(anomaly_identifiers)
    zero_count = sum(1 for v in anomaly_identifiers.values() if v == 'zero')
    double_count = sum(1 for v in anomaly_identifiers.values() if v == 'double')
    normal_count = sum(1 for v in anomaly_identifiers.values() if v == 'normal')

    logger.info(f"Anomaly detection statistics: Total {total_strings} strings, "
                f"Zero current {zero_count} strings, "
                f"Double current {double_count} strings, "
                f"Normal {normal_count} strings")


def save_history_intensity(data, station_name, update_time, repo_abs_path):
    """
    保存组串历史七天电流数据（按小时平均）
    
    Args:
        data: 已读取的30天数据
        station_name: 电站名称
        update_time: 更新时间
        repo_abs_path: 项目根路径
    """
    date_str = update_time.strftime('%Y-%m-%d')
    results_folder = os.path.join(repo_abs_path, 'data', station_name, 'results')
    
    # 确保目录存在
    os.makedirs(results_folder, exist_ok=True)
    
    json_file_path = os.path.join(results_folder, f'{date_str}.json')
    
    # 计算七天的时间范围
    end_time = update_time.replace(hour=23, minute=59, second=59)
    start_time = update_time - timedelta(days=6)  # 往前数7天（包括当天）
    start_time = start_time.replace(hour=0, minute=0, second=0)
    
    # 从已有数据中筛选出七天的数据
    history_data = []
    for row in data:
        timestamp, string_id, inverter_id, box_id, intensity = row
        dt = datetime.fromtimestamp(timestamp)
        if start_time <= dt <= end_time:
            history_data.append(row)
    
    # 处理历史数据，按组串和小时进行分组
    grouped_data = defaultdict(lambda: defaultdict(list))
    
    for row in history_data:
        timestamp, string_id, inverter_id, box_id, intensity = row
        dt = datetime.fromtimestamp(timestamp)
        unique_key = f"{box_id}-{inverter_id}-{string_id}"
        
        # 处理强度值
        if intensity == '' or intensity is None:
            intensity = 0.0
        else:
            try:
                intensity = float(intensity)
            except ValueError:
                intensity = 0.0
        
        # 按日期和小时分组
        date_hour_key = dt.strftime('%Y-%m-%d-%H')
        grouped_data[unique_key][date_hour_key].append(intensity)
    
    # 计算每个组串每小时的平均值
    history_intensity_data = {}
    
    for unique_key, hour_data in grouped_data.items():
        intensity_list = []
        
        # 生成7天×24小时的完整时间序列
        current_time = start_time
        while current_time <= end_time:
            date_hour_key = current_time.strftime('%Y-%m-%d-%H')
            
            if date_hour_key in hour_data and hour_data[date_hour_key]:
                # 计算该小时的平均值
                avg_intensity = sum(hour_data[date_hour_key]) / len(hour_data[date_hour_key])
                intensity_list.append(round(avg_intensity, 4))
            else:
                # 没有数据则填充0
                intensity_list.append(0.0)
            
            current_time += timedelta(hours=1)
        
        # 确保数据长度为168（7天×24小时）
        while len(intensity_list) < 168:
            intensity_list.append(0.0)
        intensity_list = intensity_list[:168]  # 截取前168个点
        
        history_intensity_data[unique_key] = {
            'history_intensity': intensity_list
        }
    
    # 保存到JSON文件
    if os.path.exists(json_file_path):
        # 文件存在，读取并更新
        with open(json_file_path, 'r', encoding='utf-8') as json_file:
            try:
                data = json.load(json_file)
                
                # 确保results字段存在
                if 'results' not in data:
                    data['results'] = {}
                
                # 为每个组串添加history_intensity字段
                for key, intensity_info in history_intensity_data.items():
                    if key in data['results']:
                        # 如果组串已存在，添加history_intensity字段
                        data['results'][key]['history_intensity'] = intensity_info['history_intensity']
                    else:
                        # 如果组串不存在，创建新条目
                        data['results'][key] = intensity_info
                
                # 写回文件
                with open(json_file_path, 'w', encoding='utf-8') as json_file:
                    json.dump(data, json_file, ensure_ascii=False)

                logger.info(f"Historical current data has been updated to the existing file: {json_file_path}")

            except json.JSONDecodeError:
                logger.error('Error decoding JSON from the file.')
    else:
        # 文件不存在，创建新文件
        new_data = {
            'date': date_str,
            'results': history_intensity_data
        }
        
        with open(json_file_path, 'w', encoding='utf-8') as json_file:
            json.dump(new_data, json_file, ensure_ascii=False)

        logger.info(f"Historical current data has been saved to the new file: {json_file_path}")

    # 统计信息
    total_strings = len(history_intensity_data)
    logger.info(f"Historical current data saving completed: {total_strings} strings, each with 168 hourly data points")

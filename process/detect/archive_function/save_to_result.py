import json
from datetime import datetime
import os
import pandas as pd

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
SAVE_DIR = os.path.join(PROJECT_ROOT, "data")

def construct_result_template(station_data: dict, end_date: str, station_name) ->dict:
    """
    构造结果数据模板
    Args:
        station_data: 原始电气量数据字典
        end_date: 结果日期，默认使用 END_DATE
    Returns:
        dict: 包含初始化结构的字典
    """
    result_template = {
        "date": end_date,
        "results": {}
    }
    
    # 需要跳过的箱变列表
    #skip_box_list = ["002", "003", "004", "005", "006", "007", "008", "011", "015", "038"]
    SELECT_BOX = ["012","013"]

    for key in station_data.keys():
        box_id, inv_id = key.split("-")
        
        # 跳过指定的箱变
        if box_id not in SELECT_BOX:
            continue
        df = station_data[key]
        for col in df.columns:
            if '输入电流' in col:
                string_id = col.split('输入电流')[0].replace('PV', '').zfill(3)
                device_id = f"{box_id.zfill(3)}-{inv_id.zfill(3)}-{string_id}"

                result_template["results"][device_id] = {
                    "string_id": string_id,
                    "inverter_id": inv_id.zfill(3),
                    "box_id": box_id.zfill(3),
                    "anomaly_identifier": None,  # 待填充
                    "degradation_rate": None,    # 待填充
                    "anomaly_score": None,       # 待填充
                    "rdc_posistion": [],         # 待填充
                    "anomaly_dates": []          # 待填充
                }
    
    save_dir = os.path.join(SAVE_DIR, station_name, "results")
    file_path = os.path.join(save_dir, f"{end_date}.json")
    
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(result_template, f, indent=4, ensure_ascii=False)
       
    return result_template

def update_anomaly_scores(json_path, anomaly_data):
    """
    更新JSON文件中的异常分数
    
    Args:
        json_path (str): JSON文件路径
        anomaly_data (list): 异常分数数据列表，每个元素应包含 'pid' 和 'count' 字段
    """
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    for row in anomaly_data:
        device_id = row['pid']  
        if device_id in data['results']:
            data['results'][device_id]['anomaly_score'] = float(row['count'])
    
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

def update_degradation_rates(json_path, degradation_data):
    """
    更新JSON文件中的劣化率
    
    Args:
        json_path (str): 目标JSON文件路径
        degradation_data (dict): 劣化率数据字典，格式为 {pid: rate}
    """
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    for device_id, rate in degradation_data.items():
        if device_id in data['results']:
            formatted_rate = f"{float(rate) * 100:.2f}%"
            data['results'][device_id]['degradation_rate'] = formatted_rate
    
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

def update_anomaly_dates(json_path, anomaly_dates_data):
    """
    更新JSON文件中的异常日期数组
    
    Args:
        json_path (str): 目标JSON文件路径
        anomaly_dates_data (dict): 异常日期数据字典，格式为 {bt_id: dates_list}
    """
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    for device_id, dates in anomaly_dates_data.items():
        if device_id in data['results']:
            # 如果日期超过30个，只保留最后30个
            if len(dates) > 30:
                dates = dates[-30:]
                print(f"设备 {device_id} 的异常日期超过30个，已截取最后30个日期。")
            data['results'][device_id]['anomaly_dates'] = dates
    
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

def update_identifier(json_path, identifier_data):
    """
    更新JSON文件中的异常标识符
    
    Args:
        json_path (str): JSON文件路径
        identifier_data (list): 标识符数据列表，每个元素应包含 box_id, inverter_id, string_id, error_type
    """
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    for row in identifier_data:
        device_id = f"{row['box_id'].zfill(3)}-{row['inverter_id'].zfill(3)}-{row['string_id'].zfill(3)}"
        
        if device_id in data['results']:
            # 确定异常标识符
            if not row['error_type']:
                anomaly_identifier = "normal"
            elif row['error_type'] == 'zero_current':
                anomaly_identifier = "zero"
            elif row['error_type'] == 'double_current':
                anomaly_identifier = "double"
            else:
                anomaly_identifier = "normal"
            
            data['results'][device_id]['anomaly_identifier'] = anomaly_identifier
    
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

def update_rdc_positions(json_path, rdc_data):
    """
    更新JSON文件中的降维坐标
    
    Args:
        json_path (str): 目标JSON文件路径
        rdc_data (dict): 降维坐标数据字典，格式为 {bt_id: {'x': [...], 'y': [...]}}
    """
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    for device_id, coords in rdc_data.items():
        if device_id in data['results']:
            x_coords = coords['x'][-30:] if len(coords['x']) > 30 else coords['x']
            y_coords = coords['y'][-30:] if len(coords['y']) > 30 else coords['y']
            
            while len(x_coords) < 30:
                x_coords.append(x_coords[-1] if x_coords else 0.5)
            while len(y_coords) < 30:
                y_coords.append(y_coords[-1] if y_coords else 0.5)
            
            data['results'][device_id]['rdc_posistion'] = list(zip(x_coords, y_coords))
    
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

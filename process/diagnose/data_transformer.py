import pandas as pd
import numpy as np
from datetime import datetime, time, timedelta
from collections import defaultdict
from process.diagnose.utils import is_zero_i, is_double_i
from process.diagnose.common import ZERO_THRESHOLD, ZERO_RATE, ZERO_HOUR, DOUBLE_RATE
import logging

# 日志配置（只需在模块顶部配置一次即可）
logger = logging.getLogger(__name__)

def trans_data_byStation(data, anomaly_identifiers=None):
    sample_data = data
    station_sample = trans_to_sample_byStation(sample_data, anomaly_identifiers)
    return station_sample

def trans_to_sample_byStation(sample_data, anomaly_identifiers=None):
    sample = defaultdict(list)
    day_count = 30
    
    if day_count > 0:
        # 按组串分组数据
        grouped_data = defaultdict(list)
        for entry in sample_data:
            timestamp, string_id, inverter_id, box_id, intensity = entry
            dt = datetime.fromtimestamp(timestamp)
            unique_key = f"{box_id}-{inverter_id}-{string_id}"
            
            # 如果提供了异常标识，跳过零电流和双倍电流的组串
            if anomaly_identifiers and unique_key in anomaly_identifiers:
                if anomaly_identifiers[unique_key] in ['zero', 'double']:
                    continue  # 跳过异常组串
            
            # 处理强度值
            if intensity == '' or intensity is None:
                intensity = 0.0
            else:
                try:
                    intensity = float(intensity)
                except ValueError:
                    intensity = 0.0
            
            grouped_data[unique_key].append((dt, intensity))
        
        # 为每个组串处理数据（仅处理正常组串）
        for unique_key, data_points in grouped_data.items():
            if anomaly_identifiers and unique_key in anomaly_identifiers:
                if anomaly_identifiers[unique_key] in ['zero', 'double']:
                    continue  # 跳过异常组串
            
            # 转换为DataFrame便于处理
            df = pd.DataFrame(data_points, columns=['time', 'intensity'])
            df = df.set_index('time').sort_index()
            
            # 获取数据的时间范围
            start_date = df.index.min().date()
            end_date = df.index.max().date()
            
            # 简化的数据处理逻辑 - 直接查找目标时间点
            data_list = []
            for single_date in pd.date_range(start_date, end_date):
                for hour in [10, 11, 12, 13]:  # 与训练时保持一致
                    # 定义时间段：从当前小时到下一小时
                    start_time = pd.Timestamp.combine(single_date.date(), time(hour, 0))
                    end_time = pd.Timestamp.combine(single_date.date(), time(hour + 1, 0))
                    
                    # 查找该时间段内的所有数据
                    time_range_data = df[(df.index >= start_time) & (df.index < end_time)]
                    
                    if not time_range_data.empty:
                        # 计算时间段内所有数据的平均值
                        data_list.append(time_range_data['intensity'].mean())
                    else:
                        # 没有数据则填充0
                        data_list.append(0.0)
            
            # 确保数据长度为120（30天 × 4小时）
            while len(data_list) < 120:
                data_list.append(0.0)
            data_list = data_list[:120]  # 截取前120个点
            
            sample[unique_key] = data_list
    
    # 最后统一处理reshape
    for unique_key in sample:
        arr = np.array(sample[unique_key])
        # 转换为float32并reshape为 (1, 1, length) - 与训练时的格式一致
        reshaped_arr = arr.reshape(1, 1, arr.shape[0]).astype(np.float32)
        sample[unique_key] = reshaped_arr
   
    return sample  # 只返回样本数据

def detect_anomalies_byStation(data):
    anomaly_identifiers = {}
    
    # 按箱变分组
    box_groups = defaultdict(list)
    for timestamp, string_id, inverter_id, box_id, intensity in data:
        # 处理时间戳：如果是整数（Unix时间戳），转换为datetime对象
        if isinstance(timestamp, int):
            dt = datetime.fromtimestamp(timestamp)
        else:
            # 如果是字符串，尝试解析
            try:
                dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                try:
                    dt = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S.%f')
                except ValueError:
                    logger.error(f"can not parse timestamp: {timestamp}")
                    continue
        
        # 处理强度值
        if intensity is None:
            intensity = 0.0
        
        box_groups[box_id].append({
            'time': dt,
            'string_id': string_id,
            'inverter_id': inverter_id,
            'intensity': intensity
        })
    
    
    # 对每个箱变进行处理
    for box_id, box_data in box_groups.items():
        
        # 在箱变内按逆变器分组
        inverter_groups = defaultdict(list)
        for item in box_data:
            inverter_groups[item['inverter_id']].append(item)
        
        # 对每个逆变器内的组串进行检测
        for inverter_id, inverter_data in inverter_groups.items():
            # 转换为DataFrame
            df = pd.DataFrame(inverter_data)
            
            if len(df) == 0:
                continue
            
            # 透视表：将组串ID作为列，时间作为索引，强度作为值
            try:
                pivot_df = df.pivot_table(
                    index='time', 
                    columns='string_id', 
                    values='intensity', 
                    aggfunc='mean'  # 如果同一时间有多个值，取平均
                ).fillna(0.0)
                
                
            except Exception as e:
                logger.error(f"Error creating pivot table: {e}")
                continue
            
            # 为每个组串进行检测
            for string_id in pivot_df.columns:
                unique_key = f"{box_id}-{inverter_id}-{string_id}"
                
                try:
                    # 零电流检测（检测30天内组串电流为0的占比）
                    is_zero = is_zero_i(pivot_df, str(string_id), 30)
                    
                    # 双倍电流检测（与同一逆变器内其他组串比较）
                    is_double = is_double_i(pivot_df, str(string_id))
                    
                    # 确定异常标识
                    if is_zero:
                        anomaly_identifiers[unique_key] = "zero"
                    elif is_double:
                        anomaly_identifiers[unique_key] = "double"
                    else:
                        anomaly_identifiers[unique_key] = "normal"
                        
                except Exception as e:
                    logger.error(f"Error detecting string {unique_key}: {e}")
                    anomaly_identifiers[unique_key] = "normal"  # 默认为正常
    return anomaly_identifiers
import pandas as pd
import numpy as np
from pypots.imputation import SAITS, iTransformer, FreTS
from datetime import datetime, timedelta
import time
import pytz  # 引入pytz库来处理时区
import os
from tqdm import tqdm
import logging

# 日志配置（只需在模块顶部配置一次即可）
logger = logging.getLogger(__name__)

DATU_NORMAL_VOLTAGE_IDS = ['044','046','048','050','051','054','055','056','058','060','061','062','063','064','065','066','067','068','069','070','071','072','073','074','075']

def impute_and_fill_bulk(station_name, database_manager, string_info_model, start_timestamp=None, end_timestamp=None, model_dict=None, impute_model=None, position=0):
    # 先删除同一天所有 device_id 的统计数据（只执行一次）
    with database_manager.get_session(f'{station_name}_impute') as impute_session:
        impute_session.query(impute_model).filter(impute_model.timestamp == start_timestamp).delete(synchronize_session=False)
        impute_session.commit()
    
    # 所有箱变-逆变组合处理完毕后，批量删除旧数据，批量插入新数据
    with database_manager.get_session(station_name) as session:
        # Query data within the specified time range
        query = (
            session.query(string_info_model)
            .filter(string_info_model.timestamp >= start_timestamp)
            .filter(string_info_model.timestamp <= end_timestamp)
            .order_by(string_info_model.timestamp, string_info_model.device_id)
            .all()
        )

        # Convert ORM results to DataFrame
        if query:
            df = pd.DataFrame([{
                'timestamp': item.timestamp,
                'device_id': item.device_id,
                'string_id': item.string_id,
                'inverter_id': item.inverter_id,
                'box_id': item.box_id,
                'intensity': item.intensity,
                'voltage': item.voltage,
                'fixed_intensity': item.fixed_intensity,
                'fixed_voltage': item.fixed_voltage
            } for item in query])
        else:
            df = pd.DataFrame()

        # Check if the query result is empty
        if df.empty:
            print("查询结果为空，没有数据需要处理。")
            return df

        # Group by box_id and inverter_id
        df['box_inverter_key'] = df['box_id'] + '-' + df['inverter_id']
        # 初始化一个空的 DataFrame 来存储修改后的数据
        all_updated_dfs = pd.DataFrame()

        for box_inverter_key, group_df in tqdm(df.groupby('box_inverter_key'), desc="Processing box-inverter groups", position=position):
            box_id, inverter_id = box_inverter_key.split('-')

            # step0: 对电压进行填补（仅大涂电站的部分箱变-逆变器需要）
            if station_name=='datu' and box_id not in DATU_NORMAL_VOLTAGE_IDS:
                # 仅对大涂电站的特定箱变-逆变器进行电压填补
                group_df = fill_voltage(group_df, box_id, inverter_id)
            elif station_name=='datu' and box_id in DATU_NORMAL_VOLTAGE_IDS:
                group_df = power2voltage(group_df)
            else:
                group_df = group_df.copy()

            string_ids = sorted(group_df['device_id'].unique())
            total_strings = len(string_ids)
            timestamps = sorted(group_df['timestamp'].unique())
            
            # step1: 统计数据质量，写入impute数据库
            # 按 device_id 分组处理
            device_groups = group_df.groupby('device_id')
            
            # 准备批量插入的数据
            records_to_insert = []
            
            for device_id, device_df in device_groups:
                # 数据验证和异常检测
                error_count_intensity = 0
                missing_count_intensity = 0
                error_count_voltage = 0
                missing_count_voltage = 0
                
                # 检查电流和电压
                for variable in ['intensity', 'voltage']:
                    outlier_threshold = 10 if variable == 'intensity' else 1000
                    value_series = pd.to_numeric(device_df[variable], errors='coerce')
                    
                    # 处理无法转换为数值的情况
                    nan_mask = value_series.isna()
                    nan_count = nan_mask.sum()
                    
                    # 处理负值
                    negative_mask = value_series < 0
                    negative_count = negative_mask.sum()
                    
                    # 找到异常值 - 检查前后值的变化
                    diff_with_previous = value_series.diff()
                    diff_with_next = value_series.diff(-1)
                    outlier_mask = (
                        (diff_with_previous > outlier_threshold) &
                        (diff_with_next > outlier_threshold)
                    ) | (
                        (diff_with_previous < -outlier_threshold) &
                        (diff_with_next < -outlier_threshold)
                    )
                    outlier_count = outlier_mask.sum()
                    
                    # 处理缺失值
                    non_zero_mask = (value_series != 0) & (~value_series.isna())
                    non_zero_cumsum = non_zero_mask.cumsum()
                    zero_mask = (non_zero_cumsum > 0) & (value_series == 0) & (non_zero_cumsum < non_zero_cumsum.iloc[-1])
                    missing_count = zero_mask.sum()
                    
                    # 统计错误和缺失
                    if variable == 'intensity':
                        error_count_intensity = negative_count + outlier_count
                        missing_count_intensity = missing_count + nan_count
                    else:
                        error_count_voltage = negative_count + outlier_count
                        missing_count_voltage = missing_count + nan_count
                
                # 确保所有计数变量都有默认值并转换为标准int类型
                error_count_intensity = int(error_count_intensity or 0)
                missing_count_intensity = int(missing_count_intensity or 0)
                error_count_voltage = int(error_count_voltage or 0)
                missing_count_voltage = int(missing_count_voltage or 0)
                
                # 添加到批量插入的记录列表
                records_to_insert.append({
                    'timestamp': start_timestamp,
                    'device_id': device_id,
                    'error_count_intensity': error_count_intensity,
                    'missing_count_intensity': missing_count_intensity,
                    'error_count_voltage': error_count_voltage,
                    'missing_count_voltage': missing_count_voltage
                })
            
            # 2. 删除/写入 impute 数据
            with database_manager.get_session(f'{station_name}_impute') as impute_session:
                if records_to_insert:
                    impute_session.bulk_insert_mappings(impute_model, records_to_insert)
                impute_session.commit()

            # print(f"成功插入 {len(records_to_insert)} 条记录到 {station_name}_impute 数据库")

            # step2: 批量数据填补
            # 清空上一个逆变器的更新数据
            updated_dfs = []

            if len(timestamps) != 24:
                print(f"时间点数量不是24个，实际有 {len(timestamps)} 个 (box_id={box_id}, inverter_id={inverter_id})")
                continue

            if total_strings <= 18:
                process_day_data(group_df, string_ids, timestamps, box_id, inverter_id, model_dict, updated_dfs)
            else:
                processed_strings = set()
                for i in range(0, total_strings, 18):
                    end_idx = min(i + 18, total_strings)
                    batch_strings = string_ids[i:end_idx]
                    new_strings = [s for s in batch_strings if s not in processed_strings]
                    if new_strings:
                        process_day_data(group_df, new_strings, timestamps, box_id, inverter_id, model_dict, updated_dfs)
                        processed_strings.update(new_strings)
            # 合并当前逆变器的更新数据
            if updated_dfs:
                updated_df = pd.concat(updated_dfs, ignore_index=True)
                # 根据device_id和timestamp去重，保留最后一次出现的行（最新的处理结果）
                updated_df = updated_df.drop_duplicates(subset=['device_id', 'timestamp'], keep='last')
                # 移除临时列
                if 'box_inverter_key' in updated_df.columns:
                    updated_df = updated_df.drop('box_inverter_key', axis=1)
                # 添加到总的更新数据中
                all_updated_dfs = pd.concat([all_updated_dfs, updated_df], ignore_index=True)

        # Batch delete and insert operations
        # 若 all_updated_dfs 不为空，则进行批量删除和插入操作
        if not all_updated_dfs.empty:
            # 删除已存在数据（事务1）
            # Batch delete records within the timestamp range
            session.query(string_info_model).filter(
                string_info_model.timestamp >= start_timestamp,
                string_info_model.timestamp <= end_timestamp
            ).delete(synchronize_session=False)
            print("已删除数据，准备插入新数据")

            # 批量插入（事务2）
            all_updated_dfs = all_updated_dfs.replace({np.nan: None})
            records = all_updated_dfs.to_dict('records')
            # Batch insert updated records
            session.bulk_insert_mappings(string_info_model, records)
            session.commit()
            print(f"已插入 {len(records)} 条新数据 到 {station_name} 数据库")

def process_day_data(df, target_strings, timestamps, box_id, inverter_id, model_dict, updated_dfs):
    """
    处理一天的数据，使用多变量填补方法
    
    Args:
        df: 当前box_inverter组的DataFrame
        target_strings: 要处理的目标组串ID列表
        timestamps: 时间戳列表，按顺序排列
        box_id: 箱变ID
        inverter_id: 逆变器ID
        model_dict: 模型字典
        updated_dfs: 用于存储更新后数据的列表
    """
    n_strings = len(target_strings)
    n_timestamps = len(timestamps)
    
    if n_strings == 0 or n_timestamps == 0:
        return
    
    # 为电流和电压分别创建多变量矩阵 (时间点 × 组串数)
    intensity_matrix = np.full((n_timestamps, n_strings), np.nan)
    voltage_matrix = np.full((n_timestamps, n_strings), np.nan)
    
    # 填充矩阵
    for col_idx, string_id in enumerate(target_strings):
        # 获取当前组串的所有时间点数据
        string_df = df[df['device_id'] == string_id]
        
        # 按时间戳排序
        string_df = string_df.sort_values('timestamp')
        
        # 创建时间戳到索引的映射
        ts_to_idx = {ts: idx for idx, ts in enumerate(timestamps)}
        
        # 确保string_df中的时间戳都在timestamps中
        valid_rows = string_df['timestamp'].isin(timestamps)
        if not valid_rows.all():
            string_df = string_df[valid_rows]
        
        # 获取每行对应的行索引
        row_indices = string_df['timestamp'].map(ts_to_idx).values
        
        # 批量填充矩阵
        if not string_df.empty:
            # 将电流和电压值转换为数值类型
            intensity_vals = pd.to_numeric(string_df['intensity'], errors='coerce').values
            voltage_vals = pd.to_numeric(string_df['voltage'], errors='coerce').values
            
            # 批量填充矩阵
            intensity_matrix[row_indices, col_idx] = intensity_vals
            voltage_matrix[row_indices, col_idx] = voltage_vals
    # 数据清洗 - 处理异常值
    # 1. 处理负值
    intensity_matrix[intensity_matrix < 0] = np.nan
    voltage_matrix[voltage_matrix < 0] = np.nan
    
    # 2. 处理异常波动
    # 计算沿时间方向（行方向）的一阶差分
    diff_with_previous = np.zeros_like(intensity_matrix)
    diff_with_next = np.zeros_like(intensity_matrix)
    
    # 计算与前一个时间点的差分（跳过首行）
    diff_with_previous[1:, :] = intensity_matrix[1:, :] - intensity_matrix[:-1, :]
    
    # 计算与后一个时间点的差分（跳过末行）
    diff_with_next[:-1, :] = intensity_matrix[:-1, :] - intensity_matrix[1:, :]
    
    # 标记异常点
    outlier_threshold = 10  # 电流异常阈值
    outlier_mask = (
        (diff_with_previous > outlier_threshold) & 
        (diff_with_next > outlier_threshold)
    ) | (
        (diff_with_previous < -outlier_threshold) & 
        (diff_with_next < -outlier_threshold)
    )
    
    # 将异常点置为NaN
    intensity_matrix[outlier_mask] = np.nan
    
    # 对电压也做同样处理
    diff_with_previous = np.zeros_like(voltage_matrix)
    diff_with_next = np.zeros_like(voltage_matrix)
    diff_with_previous[1:, :] = voltage_matrix[1:, :] - voltage_matrix[:-1, :]
    diff_with_next[:-1, :] = voltage_matrix[:-1, :] - voltage_matrix[1:, :]
    
    outlier_threshold = 1000  # 电压异常阈值
    outlier_mask = (
        (diff_with_previous > outlier_threshold) & 
        (diff_with_next > outlier_threshold)
    ) | (
        (diff_with_previous < -outlier_threshold) & 
        (diff_with_next < -outlier_threshold)
    )
    
    voltage_matrix[outlier_mask] = np.nan
    
    # 3. 处理零值（只处理首尾非零值之间的零值）
    # 创建一个布尔掩码标记非零且非NaN的值
    non_zero_mask_intensity = (intensity_matrix != 0) & (~np.isnan(intensity_matrix))
    non_zero_mask_voltage = (voltage_matrix != 0) & (~np.isnan(voltage_matrix))
    
    # 按列处理
    for col in range(n_strings):
        # 处理电流
        # 如果该列全是NaN或没有非零值，跳过处理
        if not np.all(~non_zero_mask_intensity[:, col]):
            # 计算累积和，用于标记第一个非零值之后的位置
            cumsum_nonzero = np.cumsum(non_zero_mask_intensity[:, col])
            # 第一个非零值之后的掩码
            after_first_nonzero = cumsum_nonzero > 0
            
            # 计算逆向累积和，用于标记最后一个非零值之前的位置
            reverse_cumsum = np.cumsum(non_zero_mask_intensity[::-1, col])[::-1]
            # 最后一个非零值之前的掩码
            before_last_nonzero = reverse_cumsum > 0
            
            # 组合条件：零值 && 在第一个非零值之后 && 在最后一个非零值之前
            middle_zeros = (intensity_matrix[:, col] == 0) & after_first_nonzero & before_last_nonzero
            
            # 将中间的零值替换为NaN
            intensity_matrix[middle_zeros, col] = np.nan
        
        # 处理电压
        # 如果该列全是NaN或没有非零值，跳过处理
        if not np.all(~non_zero_mask_voltage[:, col]):
            # 计算累积和，用于标记第一个非零值之后的位置
            cumsum_nonzero = np.cumsum(non_zero_mask_voltage[:, col])
            # 第一个非零值之后的掩码
            after_first_nonzero = cumsum_nonzero > 0
            
            # 计算逆向累积和，用于标记最后一个非零值之前的位置
            reverse_cumsum = np.cumsum(non_zero_mask_voltage[::-1, col])[::-1]
            # 最后一个非零值之前的掩码
            before_last_nonzero = reverse_cumsum > 0
            
            # 组合条件：零值 && 在第一个非零值之后 && 在最后一个非零值之前
            middle_zeros = (voltage_matrix[:, col] == 0) & after_first_nonzero & before_last_nonzero
            
            # 将中间的零值替换为NaN
            voltage_matrix[middle_zeros, col] = np.nan
    
    # 无论是否有NaN值，都进行处理
    # 如果有NaN值，先进行填补
    filled_intensity = intensity_matrix.copy()
    filled_voltage = voltage_matrix.copy()

    # 处理电流矩阵
    if np.isnan(intensity_matrix).any():
        # 填补电流矩阵
        filled_intensity = fill_matrix_with_models(intensity_matrix, model_dict, 0)  # 0表示电流

    # 处理电压矩阵
    if np.isnan(voltage_matrix).any():
        # 填补电压矩阵
        filled_voltage = fill_matrix_with_models(voltage_matrix, model_dict, 1)  # 1表示电压

    # 创建一个字典来跟踪已添加到updated_dfs的数据
    processed_rows = {}

    # 统一处理电流和电压数据
    for col_idx, string_id in enumerate(target_strings):
        for row_idx, ts in enumerate(timestamps):
            # 找到对应的原始数据行
            mask = (df['device_id'] == string_id) & (df['timestamp'] == ts)
            if mask.any():
                # 创建唯一键来标识这行数据
                row_key = f"{string_id}_{ts}"
                
                # 如果这行数据还没被处理过
                if row_key not in processed_rows:
                    # 创建一个副本以避免SettingWithCopyWarning
                    row_data = df[mask].copy()
                    
                    # 处理电流值
                    if np.isnan(intensity_matrix[row_idx, col_idx]) and not np.isnan(filled_intensity[row_idx, col_idx]):
                        # 对于NaN值，使用填补的值
                        row_data['fixed_intensity'] = filled_intensity[row_idx, col_idx]
                    else:
                        # 对于正常值，使用原始值
                        row_data['fixed_intensity'] = row_data['intensity']
                    
                    # 处理电压值
                    if np.isnan(voltage_matrix[row_idx, col_idx]) and not np.isnan(filled_voltage[row_idx, col_idx]):
                        # 对于NaN值，使用填补的值
                        row_data['fixed_voltage'] = filled_voltage[row_idx, col_idx]
                    else:
                        # 对于正常值，使用原始值
                        row_data['fixed_voltage'] = row_data['voltage']
                    
                    # 添加到更新列表
                    updated_dfs.append(row_data)
                    
                    # 标记为已处理
                    processed_rows[row_key] = len(updated_dfs) - 1
                else:
                    # 如果这行数据已经被处理过，获取其在updated_dfs中的索引
                    existing_idx = processed_rows[row_key]
                    
                    # 如果需要，更新电流值
                    if np.isnan(intensity_matrix[row_idx, col_idx]) and not np.isnan(filled_intensity[row_idx, col_idx]):
                        updated_dfs[existing_idx]['fixed_intensity'] = filled_intensity[row_idx, col_idx]
                    
                    # 如果需要，更新电压值
                    if np.isnan(voltage_matrix[row_idx, col_idx]) and not np.isnan(filled_voltage[row_idx, col_idx]):
                        updated_dfs[existing_idx]['fixed_voltage'] = filled_voltage[row_idx, col_idx]


def fill_matrix_with_models(data_matrix, model_dict, variable_type):
    """
    使用模型填补矩阵中的NaN值
    
    Args:
        data_matrix: 包含NaN值的数据矩阵，形状为(n_steps, n_features)
        model_dict: 模型字典
        variable_type: 变量类型，0表示电流，1表示电压
    
    Returns:
        填补后的矩阵
    """
    # 如果矩阵中没有NaN值，直接返回
    if not np.isnan(data_matrix).any():
        return data_matrix
    
    # 如果矩阵全为NaN，返回全0矩阵
    if np.isnan(data_matrix).all():
        return np.zeros_like(data_matrix)
    
    # 设置归一化参数
    min_value = 0
    max_value = 15 if variable_type == 0 else 1500  # 电流最大值15A，电压最大值1500V
    
    # 归一化
    data_norm = (data_matrix - min_value) / (max_value - min_value)
    
    # 确保矩阵形状符合模型要求 - 已经是(n_steps, n_features)形状
    n_steps, n_features = data_norm.shape
    
    # 如果特征数量不足18个，需要填充
    if n_features < 18:
        # 创建一个全NaN的矩阵，形状为(n_steps, 18)
        padded_data = np.full((n_steps, 18), np.nan)
        # 将原始数据复制到新矩阵的前面几列
        padded_data[:, :n_features] = data_norm
        # 更新数据和特征数
        data_norm = padded_data
        n_features = 18
    # 如果特征数量超过18个，只使用前18个特征
    elif n_features > 18:
        data_norm = data_norm[:, :18]
        n_features = 18
    
    # 准备模型输入 - 添加批次维度
    data_norm = data_norm.reshape(1, n_steps, n_features)
    dataset_for_testing = {"X": data_norm}
    
    # 使用多个模型进行填补，选择最佳结果
    results = []
    
    # 使用SAITS模型
    if 'SAITS' in model_dict:
        saits = model_dict['SAITS']
        imputation = saits.impute(dataset_for_testing)
        imputation_results = imputation * (max_value - min_value) + min_value
        results.append(imputation_results)
    # 使用iTransformer模型
    if 'iTransformer' in model_dict:
        itransformer = model_dict['iTransformer']
        imputation = itransformer.impute(dataset_for_testing)
        imputation_results = imputation * (max_value - min_value) + min_value
        results.append(imputation_results)
    
    # 使用FreTS模型
    if 'FreTS' in model_dict:
        frets = model_dict['FreTS']
        imputation = frets.impute(dataset_for_testing)
        imputation_results = imputation * (max_value - min_value) + min_value
        results.append(imputation_results)
    
    # 选择最佳结果（这里先直接取saits的结果）
    filled_matrix = results[0]
    # 确保值非负
    filled_matrix = np.maximum(filled_matrix, 0)
    
    # 只填补原始矩阵中的NaN值
    # 首先处理可能的尺寸不匹配问题
    original_shape = data_matrix.shape
    if filled_matrix.shape[2] != original_shape[1]:
        # 只使用原始矩阵对应的列
        filled_data = filled_matrix[0, :, :original_shape[1]]
    else:
        filled_data = filled_matrix[0]
    
    # 创建掩码并填补
    mask = np.isnan(data_matrix)
    result = data_matrix.copy()
    result[mask] = filled_data[mask]
    return result


def get_time_range(process_date, previous_day=0):
    # 解析输入的日期字符串为 datetime 对象
    end_date_obj = datetime.strptime(process_date, '%Y-%m-%d')

    # 设置上海时区
    shanghai_tz = pytz.timezone('Asia/Shanghai')
    end_date_obj = shanghai_tz.localize(end_date_obj)

    start_date_obj = end_date_obj - timedelta(days=previous_day)

    start_timestamp = int(time.mktime(start_date_obj.replace(hour=0, minute=0, second=0, microsecond=0).timetuple()))
    end_timestamp = int(time.mktime(end_date_obj.replace(hour=23, minute=59, second=59, microsecond=999).timetuple()))

    return start_timestamp, end_timestamp

def power2voltage(df: pd.DataFrame) -> pd.DataFrame:
    """
    将df中的'voltage'列（实际为功率power，单位W）转换为电压（单位V）。
    新电压 = power * 1000 / intensity
    intensity为0或NaN时，结果设为NaN。
    """
    df_filled = df.copy()
    # 转为数值，避免字符串问题
    power = pd.to_numeric(df_filled['voltage'], errors='coerce')
    intensity = pd.to_numeric(df_filled['intensity'], errors='coerce')
    # 计算电压，避免除零
    with np.errstate(divide='ignore', invalid='ignore'):
        voltage = (power * 1000) / intensity
        voltage[(intensity == 0) | (intensity.isna())] = np.nan
    df_filled['voltage'] = voltage
    return df_filled

def fill_voltage(df: pd.DataFrame, box_id: str, inverter_id: str) -> pd.DataFrame:
    """
    对datuStringInfo的DataFrame进行电压填补，仅填补voltage字段，其它字段保持原有值。
    组串号从"001"到"030"，部分组串可能不存在。
    填补规则：第i号（1~15）组串的电压扩充到第(i*2)和(i*2+1)号组串，若目标组串不存在则跳过。
    """
    df_filled = df.copy()
    df_filled['voltage'] = pd.to_numeric(df_filled['voltage'], errors='coerce')

    # 生成完整的string_id列表
    all_string_ids = [f"{i:03d}" for i in range(1, 31)]
    existing_string_ids = set(df['string_id'].unique())

    # 前一半（1~15）为源，后一半（16~30）为目标
    for idx in range(15):
        src_str = all_string_ids[idx]  # "001"~"015"
        tgt_str1 = all_string_ids[2 * idx]     # "001", "003", ..., "029"
        tgt_str2 = all_string_ids[2 * idx + 1] # "002", "004", ..., "030"

        # 如果源组串不存在，跳过
        if src_str not in existing_string_ids:
            continue

        src_data = df[df['string_id'] == src_str].set_index('timestamp')

        # 只对存在的目标组串扩充
        for tgt_str in [tgt_str1, tgt_str2]:
            if tgt_str not in existing_string_ids:
                continue
            tgt_mask = (
                (df_filled['box_id'] == box_id) &
                (df_filled['inverter_id'] == inverter_id) &
                (df_filled['string_id'] == tgt_str)
            )
            for ts, voltage in src_data['voltage'].items():
                row_mask = tgt_mask & (df_filled['timestamp'] == ts)
                df_filled.loc[row_mask, 'voltage'] = voltage
    return df_filled

    # 获取辐照度数据（如果可用）
    irradiance_np = None
    irradiance_df = None
    try:
        with database_manager.get_session(station_name) as irradiance_session:
            irradiance_query = (
                irradiance_session.query(station_info_model.timestamp, station_info_model.irradiance)
                .filter(station_info_model.timestamp >= timestamps[0])
                .filter(station_info_model.timestamp <= timestamps[-1])
                .order_by(station_info_model.timestamp)
                .all()
            )
            
            if irradiance_query:
                # 创建时间戳到辐照度的映射
                irradiance_map = {item.timestamp: item.irradiance for item in irradiance_query}
                # 按照timestamps的顺序获取辐照度值
                irradiance_np = np.array([float(irradiance_map.get(ts, 0)) for ts in timestamps])
                # 创建辐照度DataFrame
                irradiance_df = pd.DataFrame([{
                    'timestamp': item.timestamp,
                    'irradiance': item.irradiance
                } for item in irradiance_query])
    except Exception as e:
        logger.error(f"Error fetching irradiance data for {station_name}: {e}")
        irradiance_np = None
        irradiance_df = None
    return irradiance_np, irradiance_df
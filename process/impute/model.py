import pandas as pd
import numpy as np
from pypots.optim import Adam
from pypots.imputation import SAITS, iTransformer, FreTS, Lerp
from datetime import timedelta
import time
import pytz
import os


def impute(station_name, device_id, start_time, variable, repo_abs_path, database_manager=None, station_model=None):
    """
    填补缺失数据，采用多变量填补方式处理同一逆变器下的多个组串
    
    Args:
        station_name: 场站名称
        device_id: 设备ID
        start_time: 开始时间
        variable: 变量类型，'0'表示电流，'1'表示电压
        repo_abs_path: 项目根目录绝对路径
        database_manager: 数据库管理器实例，如果为None则使用sqlite连接
        station_model: 场站表模型元组，如果为None则使用sqlite连接
    """
    
    # 获取同一逆变器下的所有组串ID
    all_device_ids = get_strings_by_device_orm(station_name, device_id, database_manager, station_model)
    
    # 对组串ID进行排序，确保按照组串号的顺序排列
    all_device_ids.sort(key=lambda x: int(x.split('-')[2]))
    
    total_strings = len(all_device_ids)
    
    if total_strings == 0:
        print(f"错误: 找不到任何组串 (device_id={device_id})")
        return []
    
    # 确定当前组串在所有组串中的索引位置
    try:
        current_index = all_device_ids.index(device_id)
    except ValueError:
        print(f"错误: 当前组串ID不在查询结果中 (device_id={device_id})")
        return []
    
    
    # 根据当前组串位置决定使用哪些组串数据
    target_devices = []
    if total_strings <= 18:
        # 如果总数小于等于18，使用所有组串
        target_devices = all_device_ids
    elif current_index < 18:
        # 如果当前组串在前半部分，使用前18个组串
        target_devices = all_device_ids[:18]
    else:
        # 如果当前组串在后半部分，使用后18个组串
        start_idx = max(0, total_strings - 18)
        target_devices = all_device_ids[start_idx:]    
    
    # 定义上海时区
    shanghai_tz = pytz.timezone('Asia/Shanghai')
    
    # 将输入时间转换为上海时区的datetime对象
    start_datetime = pd.to_datetime(start_time).tz_localize(shanghai_tz)
    end_datetime = start_datetime + timedelta(hours=23, minutes=59, seconds=59)
    
    # 转换为时间戳
    start_timestamp = int(time.mktime(start_datetime.timetuple()))
    end_timestamp = int(time.mktime(end_datetime.timetuple()))
    
    # 根据variable选择查询的列
    value_col = "intensity" if int(variable) == 0 else "voltage"
    
    # 创建一个空的矩阵用于存储所有组串的数据 (24行 × 18列)
    multi_var_data = np.zeros((24, 18))
    multi_var_data.fill(np.nan)  # 初始化为NaN
    
    # 记录当前组串的原始数据和时间信息，用于结果输出
    device_data = None
    device_times = None
    
    # 使用ORM中间件连接数据库
    _, _, string_info_model = station_model
    with database_manager.get_session(station_name) as session:
        # 查询一天24小时的所有时间戳
        time_query = (
            session.query(string_info_model.timestamp)
            .filter(string_info_model.device_id == device_id)
            .filter(string_info_model.timestamp >= start_timestamp)
            .filter(string_info_model.timestamp < end_timestamp)
            .order_by(string_info_model.timestamp)
            .distinct()
            .all()
        )
        
        # 提取时间戳并转换为datetime对象
        timestamps = [item.timestamp for item in time_query]
        datetimes = [pd.to_datetime(ts, unit='s').tz_localize('UTC').tz_convert('Asia/Shanghai').tz_localize(None) for ts in timestamps]
        
        # 提取小时信息，用于判断白天时间段
        hours = np.array([dt.hour for dt in datetimes])
        
        # 获取辐照度数据（如果可用）
        irradiance = None
        try:
            station_info_model, _, _ = station_model
            irradiance_query = (
                session.query(station_info_model.timestamp, station_info_model.irradiance)
                .filter(station_info_model.timestamp >= start_timestamp)
                .filter(station_info_model.timestamp < end_timestamp)
                .order_by(station_info_model.timestamp)
                .all()
            )
            
            if irradiance_query:
                # 创建时间戳到辐照度的映射
                irradiance_map = {item.timestamp: item.irradiance for item in irradiance_query}
                # 按照timestamps的顺序获取辐照度值
                irradiance = np.array([float(irradiance_map.get(ts, 0)) for ts in timestamps])
        except Exception as e:
            print(f"获取辐照度数据时出错: {e}")
            irradiance = None
        
        # 如果没有足够的时间点，或者完全没有数据
        if len(timestamps) == 0:
            print(f"错误: 未找到当前组串的时间点数据 (device_id={device_id})")
            return []
        
        # 如果时间点不足24个，记录下来（这可能需要特殊处理）
        if len(timestamps) < 24:
            print(f"警告: 时间点数量不足24个，实际有 {len(timestamps)} 个")
        
        # 遍历所有目标组串
        for col_idx, string_id in enumerate(target_devices):
            if string_id is None:
                # 如果是None，表示这列应该用NaN填充
                continue
                
            # 根据variable选择查询的列
            if value_col == "intensity":
                query = (
                    session.query(string_info_model.timestamp, string_info_model.intensity)
                    .filter(string_info_model.device_id == string_id)
                    .filter(string_info_model.timestamp >= start_timestamp)
                    .filter(string_info_model.timestamp < end_timestamp)
                    .order_by(string_info_model.timestamp)
                    .all()
                )
                
                # 存储数据到矩阵中
                for result in query:
                    ts = result.timestamp
                    try:
                        row_idx = timestamps.index(ts)
                        value = pd.to_numeric(result.intensity, errors='coerce')
                        
                        # 如果是当前组串，保存原始数据
                        if string_id == device_id:
                            # 如果是第一次遇到当前组串的数据，初始化array
                            if device_data is None:
                                device_data = np.full(len(timestamps), np.nan)
                                device_times = datetimes
                            
                            device_data[row_idx] = value
                        
                        # 存储到多变量矩阵中
                        if row_idx < multi_var_data.shape[0] and col_idx < multi_var_data.shape[1]:
                            multi_var_data[row_idx, col_idx] = value
                    except ValueError:
                        # 时间戳不在timestamps列表中，忽略这条数据
                        pass
            else:
                query = (
                    session.query(string_info_model.timestamp, string_info_model.voltage)
                    .filter(string_info_model.device_id == string_id)
                    .filter(string_info_model.timestamp >= start_timestamp)
                    .filter(string_info_model.timestamp < end_timestamp)
                    .order_by(string_info_model.timestamp)
                    .all()
                )
                
                # 存储数据到矩阵中
                for result in query:
                    ts = result.timestamp
                    try:
                        row_idx = timestamps.index(ts)
                        value = pd.to_numeric(result.voltage, errors='coerce')
                        
                        # 如果是当前组串，保存原始数据
                        if string_id == device_id:
                            # 如果是第一次遇到当前组串的数据，初始化array
                            if device_data is None:
                                device_data = np.full(len(timestamps), np.nan)
                                device_times = datetimes
                            
                            device_data[row_idx] = value
                        
                        # 存储到多变量矩阵中
                        if row_idx < multi_var_data.shape[0] and col_idx < multi_var_data.shape[1]:
                            multi_var_data[row_idx, col_idx] = value
                    except ValueError:
                        # 时间戳不在timestamps列表中，忽略这条数据
                        pass
    # 检查当前组串的数据是否存在
    if device_data is None:
        print(f"错误: 未找到当前组串的有效数据 (device_id={device_id})")
        return []
    
    
    # 设置异常值检测的阈值
    outlier_threshold = 10 if int(variable) == 0 else 1000
    
    # 对整个矩阵进行数据清洗 - 使用向量化操作
    
    # 1. 处理负值（将负值转换为NaN）
    multi_var_data[multi_var_data < 0] = np.nan
    
    # 2. 处理异常波动（检查前后值的变化）
    # 计算沿时间方向（行方向）的一阶差分
    diff_with_previous = np.zeros_like(multi_var_data)
    diff_with_next = np.zeros_like(multi_var_data)
    
    # 计算与前一个时间点的差分（跳过首行）
    diff_with_previous[1:, :] = multi_var_data[1:, :] - multi_var_data[:-1, :]
    
    # 计算与后一个时间点的差分（跳过末行）
    diff_with_next[:-1, :] = multi_var_data[:-1, :] - multi_var_data[1:, :]
    
    # 标记异常点
    outlier_mask = (
        (diff_with_previous > outlier_threshold) & 
        (diff_with_next > outlier_threshold)
    ) | (
        (diff_with_previous < -outlier_threshold) & 
        (diff_with_next < -outlier_threshold)
    )
    
    # 将异常点置为NaN
    multi_var_data[outlier_mask] = np.nan
    
    # 3. 处理缺失值 - 使用与db2impute_db函数相同的方法
    # 按列处理
    for col in range(multi_var_data.shape[1]):
        # 0. 判断该列是否全为0或全为NaN
        if np.all(np.isnan(multi_var_data[:, col])) or np.all((multi_var_data[:, col] == 0) | np.isnan(multi_var_data[:, col])):
            continue
        
        # 1. 判断是否在白天时间段（9:00-17:00）
        is_daytime = (hours >= 9) & (hours <= 17)
        
        # 2. 检查当前组串的值是否为0
        current_col_zero = (multi_var_data[:, col] == 0)
        
        # 只考虑白天时间段且当前值为0的点
        potential_missing = current_col_zero & is_daytime
        
        # 如果没有潜在的缺失值，跳过后续处理
        if not np.any(potential_missing):
            continue
        
        # 3. 检查同一时间点其他组串是否有正常运行的
        # 创建一个掩码，排除当前列
        other_cols_mask = np.ones(multi_var_data.shape[1], dtype=bool)
        other_cols_mask[col] = False
        
        # 对每个时间点，检查其他列是否有正常运行的
        for row in range(multi_var_data.shape[0]):
            # 只处理潜在的缺失值
            if not potential_missing[row]:
                continue
                
            # 检查其他列在同一时间点是否有任何非零且非NaN值
            other_cols_running = np.any((multi_var_data[row, other_cols_mask] != 0) & 
                                       (~np.isnan(multi_var_data[row, other_cols_mask])))
            
            # 如果其他列没有正常运行，则不认为是缺失值
            if not other_cols_running:
                potential_missing[row] = False
                continue
                
            # 4. 检查辐照度（如果可用）
            if irradiance is not None and row < len(irradiance):
                # 如果辐照度为0，则不认为是缺失值
                if irradiance[row] == 0:
                    potential_missing[row] = False
        
        # 将确认的缺失值置为NaN
        multi_var_data[potential_missing, col] = np.nan
        
    
    # 更新当前组串的处理后数据
    current_col_index = target_devices.index(device_id)
    device_data = multi_var_data[:, current_col_index].copy()
    
    # 在归一化之前，检查当前组串数据状态
    current_string_data = multi_var_data[:, current_col_index].copy()

    # 检查当前组串是否有缺失值
    if not np.isnan(current_string_data).any():
        # 直接返回原始数据
        impute_res = []
        for i, timestamp in enumerate(device_times):
            impute_res.append({
                'date': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                'value': float(max(current_string_data[i], 0))  # 确保值非负
            })
        return [{"model": "original", "auc": 1, "impute": impute_res}]

    # 如果部分缺失，继续执行模型填补
    min_value = 0
    # 设定了一个大概的最大值用于归一化，所选数据段的最大值可能很小，不适合归一化
    max_value = 15 if int(variable) == 0 else 1500
    # 归一化
    multi_var_data_norm = (multi_var_data - min_value) / (max_value - min_value)
    
    n_steps = multi_var_data.shape[0]
    n_features = multi_var_data.shape[1]


    models = {
    # 'Lerp': Lerp(),  # 添加线性插值模型
    
    'SAITS': SAITS(
        n_steps=n_steps,
        n_features=n_features,
        n_layers=2,
        d_model=256,
        d_ffn=128,
        n_heads=4,
        d_k=64,
        d_v=64,
        dropout=0.1,
        epochs=20,
        batch_size=32,
        optimizer=Adam(lr=1e-3),
    ),

    'iTransformer': iTransformer(
        n_steps=n_steps,
        n_features=n_features,
        n_layers=2,
        d_model=256,
        d_ffn=128,
        n_heads=4,
        d_k=64,
        d_v=64,
        dropout=0.1,
        epochs=20,
        batch_size=32,
        optimizer=Adam(lr=1e-3),
    ),
    'FreTS': FreTS(
        n_steps=n_steps,
        n_features=n_features,
        embed_size=256,
        hidden_size=256,
        channel_independence=False,
        epochs=20,
        batch_size=32,
        optimizer=Adam(lr=1e-3),
    )
}

    model_res = []
    multi_var_data_norm = multi_var_data_norm.reshape(1, n_steps, n_features)
    dataset_for_testing = {
        "X": multi_var_data_norm,
    }
    for model_name, model in models.items():
        # 加载模型
        model_load_path = os.path.join(repo_abs_path, 'process', 'impute', 'model_multivariate', f'{model_name.lower()}.pypots')
        if model_name != 'Lerp':
            model.load(model_load_path)
        imputation = model.impute(dataset_for_testing)
        imputation_results = imputation * (max_value - min_value) + min_value

        
        # 提取当前组串的填补结果
        current_string_imputation = imputation_results[0, :, current_col_index].reshape(-1)
        
        # 确保值非负
        current_string_imputation = np.maximum(current_string_imputation, 0)
        
        append_multivar_res(model_res, device_times, current_string_imputation, model_name, 1)

    return model_res[:3]


def repair(station_name, device_id, start_time, variable, repo_abs_path, database_manager=None, station_model=None):
    """
    修复缺失数据，使用线性插值方法针对单个设备进行填补
    
    Args:
        station_name: 场站名称
        device_id: 设备ID
        start_time: 开始时间
        variable: 变量类型，'0'表示电流，'1'表示电压
        repo_abs_path: 项目根目录绝对路径
        database_manager: 数据库管理器实例，如果为None则使用sqlite连接
        station_model: 场站表模型元组，如果为None则使用sqlite连接
    """
    
    # 定义上海时区
    shanghai_tz = pytz.timezone('Asia/Shanghai')
    
    # 将输入时间转换为上海时区的datetime对象
    start_datetime = pd.to_datetime(start_time).tz_localize(shanghai_tz)
    end_datetime = start_datetime + timedelta(hours=23, minutes=59, seconds=59)
    
    # 转换为时间戳
    start_timestamp = int(time.mktime(start_datetime.timetuple()))
    end_timestamp = int(time.mktime(end_datetime.timetuple()))
    
    # 根据variable选择查询的列
    value_col = "intensity" if int(variable) == 0 else "voltage"
    
    # 记录当前组串的原始数据和时间信息，用于结果输出
    device_data = None
    device_times = None
    
    # 使用ORM中间件连接数据库
    _, _, string_info_model = station_model
    with database_manager.get_session(station_name) as session:
        # 查询一天24小时的所有时间戳
        time_query = (
            session.query(string_info_model.timestamp)
            .filter(string_info_model.device_id == device_id)
            .filter(string_info_model.timestamp >= start_timestamp)
            .filter(string_info_model.timestamp < end_timestamp)
            .order_by(string_info_model.timestamp)
            .distinct()
            .all()
        )
        
        # 提取时间戳并转换为datetime对象
        timestamps = [item.timestamp for item in time_query]
        datetimes = [pd.to_datetime(ts, unit='s').tz_localize('UTC').tz_convert('Asia/Shanghai').tz_localize(None) for ts in timestamps]
        
        # 提取小时信息，用于判断白天时间段
        hours = np.array([dt.hour for dt in datetimes])
        
        # 获取辐照度数据（如果可用）
        irradiance = None
        try:
            station_info_model, _, _ = station_model
            irradiance_query = (
                session.query(station_info_model.timestamp, station_info_model.irradiance)
                .filter(station_info_model.timestamp >= start_timestamp)
                .filter(station_info_model.timestamp < end_timestamp)
                .order_by(station_info_model.timestamp)
                .all()
            )
            
            if irradiance_query:
                # 创建时间戳到辐照度的映射
                irradiance_map = {item.timestamp: item.irradiance for item in irradiance_query}
                # 按照timestamps的顺序获取辐照度值
                irradiance = np.array([float(irradiance_map.get(ts, 0)) for ts in timestamps])
        except Exception as e:
            print(f"获取辐照度数据时出错: {e}")
            irradiance = None
        
        # 如果没有足够的时间点，或者完全没有数据
        if len(timestamps) == 0:
            print(f"错误: 未找到当前组串的时间点数据 (device_id={device_id})")
            return []
        
        # 如果时间点不足24个，记录下来（这可能需要特殊处理）
        if len(timestamps) < 24:
            print(f"警告: 时间点数量不足24个，实际有 {len(timestamps)} 个")
        
        # 查询当前设备的数据
        if value_col == "intensity":
            query = (
                session.query(string_info_model.timestamp, string_info_model.intensity)
                .filter(string_info_model.device_id == device_id)
                .filter(string_info_model.timestamp >= start_timestamp)
                .filter(string_info_model.timestamp < end_timestamp)
                .order_by(string_info_model.timestamp)
                .all()
            )
        else:
            query = (
                session.query(string_info_model.timestamp, string_info_model.voltage)
                .filter(string_info_model.device_id == device_id)
                .filter(string_info_model.timestamp >= start_timestamp)
                .filter(string_info_model.timestamp < end_timestamp)
                .order_by(string_info_model.timestamp)
                .all()
            )
        
        # 初始化数据数组
        device_data = np.full(len(timestamps), np.nan)
        device_times = datetimes
        
        # 存储数据到数组中
        for result in query:
            ts = result.timestamp
            try:
                row_idx = timestamps.index(ts)
                value = pd.to_numeric(getattr(result, value_col), errors='coerce')
                device_data[row_idx] = value
            except ValueError:
                # 时间戳不在timestamps列表中，忽略这条数据
                pass
    
    # 检查当前组串的数据是否存在
    if device_data is None:
        print(f"错误: 未找到当前组串的有效数据 (device_id={device_id})")
        return []
    
    # 设置异常值检测的阈值
    outlier_threshold = 10 if int(variable) == 0 else 1000
    
    # 数据清洗 - 使用向量化操作
    
    # 1. 处理负值（将负值转换为NaN）
    device_data[device_data < 0] = np.nan
    
    # 2. 处理异常波动（检查前后值的变化）
    # 计算沿时间方向的一阶差分
    diff_with_previous = np.zeros_like(device_data)
    diff_with_next = np.zeros_like(device_data)
    
    # 计算与前一个时间点的差分（跳过首行）
    diff_with_previous[1:] = device_data[1:] - device_data[:-1]
    
    # 计算与后一个时间点的差分（跳过末行）
    diff_with_next[:-1] = device_data[:-1] - device_data[1:]
    
    # 标记异常点
    outlier_mask = (
        (diff_with_previous > outlier_threshold) & 
        (diff_with_next > outlier_threshold)
    ) | (
        (diff_with_previous < -outlier_threshold) & 
        (diff_with_next < -outlier_threshold)
    )
    
    # 将异常点置为NaN
    device_data[outlier_mask] = np.nan
    print(device_data)
    
    
    # 修正判断条件：检查是否全为0，或者是否全为NaN
    all_zero = np.all(device_data == 0)
    all_nan = np.all(np.isnan(device_data))
    
    print(f"all_zero: {all_zero}")
    print(f"all_nan: {all_nan}")
    
    if all_zero or all_nan:
        # 如果全为0或NaN，直接返回原始数据
        impute_res = []
        for i, timestamp in enumerate(device_times):
            impute_res.append({
                'date': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                'value': float(max(device_data[i] if not np.isnan(device_data[i]) else 0, 0))
            })
        return [{"model": "original", "auc": 1, "impute": impute_res}]
    

    # 检查当前组串是否有缺失值
    if not np.isnan(device_data).any():
        # 直接返回原始数据
        impute_res = []
        for i, timestamp in enumerate(device_times):
            impute_res.append({
                'date': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                'value': float(max(device_data[i], 0))  # 确保值非负
            })
        return [{"model": "zero_fill", "auc": 1, "impute": impute_res}]

    # 如果部分缺失，使用线性插值进行填补
    # 准备数据用于线性插值
    device_data_reshaped = device_data.reshape(1, -1, 1)  # 形状: (batch_size, n_steps, n_features)
    dataset_for_testing = {"X": device_data_reshaped}
    # 使用线性插值模型
    lerp_model = Lerp()
    imputation = lerp_model.impute(dataset_for_testing)
    imputation_results = imputation.reshape(-1)  # 展平为一维数组
    
    # 确保值非负
    imputation_results = np.maximum(imputation_results, 0)
    
    # 生成填补结果
    impute_res = []
    for i, timestamp in enumerate(device_times):
        impute_res.append({
            'date': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            'value': float(imputation_results[i])
        })
    
    return [{"model": "Lerp", "auc": 1, "impute": impute_res}]


def append_multivar_res(model_res, timestamps, imputation, model_name, auc):
    """
    将多变量模型填补结果添加到结果列表中
    
    Args:
        model_res: 结果列表
        timestamps: 时间戳列表
        imputation: 填补结果（24个点）
        model_name: 模型名称
        auc: 准确率
    """
    # 生成填补结果
    impute_res = []
    for i, timestamp in enumerate(timestamps):
        impute_res.append({
            'date': timestamp.strftime('%Y-%m-%d %H:%M:%S'),  # 转换为标准日期时间字符串
            'value': float(max(imputation[i], 0))  # 确保值非负
        })
    
    # 添加到结果列表
    model_res.append({
        'model': model_name,
        'auc': auc,
        'impute': impute_res
    })
    return model_res

def get_strings_by_device_orm(station_name, device_id, database_manager, station_model):
    """
    使用ORM获取与指定设备相同箱变和逆变器的所有组串ID
    
    参数：
    - station_name: 场站名称
    - device_id: 设备ID，形如 "003-004-005"
    - database_manager: 数据库管理器实例
    - station_model: 场站表模型元组
    
    返回：
    - 设备ID数组
    """
    # 获取字符串信息模型
    _, _, string_info_model = station_model
    
    # 从device_id中提取箱变号和逆变器号
    parts = device_id.split('-')
    if len(parts) != 3:
        print(f"Error: Invalid device_id format '{device_id}', expected format 'box-inverter-string'")
        return []
        
    box_id = parts[0]
    inverter_id = parts[1]
    
    # 构建device_id前缀用于LIKE查询
    device_id_prefix = f"{box_id}-{inverter_id}-"
    

    # 使用带有表达式缓存的会话
    with database_manager.get_session(station_name) as session:
        from sqlalchemy import distinct
        
        # # 使用DISTINCT直接在数据库层面去重（耗时）
        # query = (
        #     session.query(distinct(string_info_model.device_id))
        #     .filter(string_info_model.device_id.startswith(device_id_prefix))
        # )
        # result = [row[0] for row in query]


        query = (
            session.query(string_info_model.device_id)
            .filter(string_info_model.device_id.startswith(device_id_prefix))
            .limit(100)  # 限制返回结果数量，避免过多数据
        )
        result = set()
        # 执行查询并获取所有结果
        for row in query:
            result.add(row[0])
            
        return list(result)
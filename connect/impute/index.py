import pandas as pd
from datetime import datetime, timedelta
from pytz import timezone

def save_imputed_result_orm(station_name, device_id, start_time, variable, impute_data, repo_abs_path, database_manager=None, station_model=None):
    """
    使用ORM方式保存填补结果到数据库
    
    Args:
        station_name: 场站名称
        device_id: 设备ID
        start_time: 开始时间 (格式: 'YYYY-MM-DD')
        variable: 变量类型 ('0'为intensity, '1'为voltage)
        impute_data: 24小时的填补数据数组
        repo_abs_path: 项目根目录绝对路径
        database_manager: 数据库管理器实例
        station_model: 场站表模型元组
    
    Returns:
        dict: 包含操作结果的字典，成功为{'code': 200, 'message': 'Success'}
    """
    try:        
        # 获取字符串信息模型
        _, _, string_info_model = station_model
        
        # 生成24小时的时间戳，考虑时区
        start_datetime = pd.to_datetime(start_time)
        # 转换为上海时区
        local_tz = timezone('Asia/Shanghai')
        start_datetime = local_tz.localize(start_datetime)
        # 转换成UTC+8时间
        print("current timestamp is:", int(start_datetime.timestamp()))
        utc_start = start_datetime
        
        # 生成24个小时的时间戳
        timestamps = []
        for i in range(24):
            hour_datetime = utc_start + timedelta(hours=i)
            timestamps.append(int(hour_datetime.timestamp()))
        
        # 根据variable选择更新的列
        is_intensity = int(variable) == 0
        
        # 使用ORM中间件连接数据库并更新数据
        with database_manager.get_session(station_name) as session:
            from sqlalchemy import update
            
            # 批量更新数据，保留两位小数
            for timestamp, value in zip(timestamps, impute_data):
                # 四舍五入到两位小数
                rounded_value = round(float(value), 2)
                
                # 根据variable选择更新的列
                if is_intensity:
                    # 使用ORM查询并更新
                    query = (
                        session.query(string_info_model)
                        .filter(string_info_model.device_id == device_id)
                        .filter(string_info_model.timestamp == timestamp)
                    )
                    
                    record = query.first()
                    if record:
                        record.fixed_intensity = rounded_value
                else:
                    # 使用ORM查询并更新
                    query = (
                        session.query(string_info_model)
                        .filter(string_info_model.device_id == device_id)
                        .filter(string_info_model.timestamp == timestamp)
                    )
                    
                    record = query.first()
                    if record:
                        record.fixed_voltage = rounded_value
            
            # 提交事务 - 自动由with语句处理
        
        return {'code': 200, 'message': 'Success'}
        
    except Exception as e:
        # 异常处理 - session的回滚由with语句自动处理
        return {
            'code': 500,
            'message': f'Error saving imputed data: {str(e)}'
        }

def check_imputed_data_orm(station_name, device_id, start_time, variable, database_manager=None, station_model=None):
    """
    使用ORM方式检查数据库中是否已有填补结果
    
    Args:
        station_name (str): 场站名称
        device_id (str): 设备ID
        start_time (str): 开始时间，格式为 'YYYY-MM-DD'
        variable (str): 变量类型，'0'表示电流，'1'表示电压
        database_manager: 数据库管理器实例
        station_model: 场站表模型元组
    
    Returns:
        list/None: 如果数据库中有填补结果，返回填补数据列表；否则返回None
    """
    try:        
        # 获取字符串信息模型
        _, _, string_info_model = station_model
        
        # 转换时间范围
        start_datetime = pd.to_datetime(start_time)
        # 转换为上海时区
        local_tz = timezone('Asia/Shanghai')
        start_datetime = local_tz.localize(start_datetime)
        end_datetime = start_datetime + timedelta(hours=23, minutes=59, seconds=59)
        
        # 转换为时间戳
        start_timestamp = int(start_datetime.timestamp())
        end_timestamp = int(end_datetime.timestamp())
        
        # 使用ORM中间件连接数据库
        with database_manager.get_session(station_name) as session:
            # 根据variable选择查询的列
            if variable == '0':
                # 查询fixed_intensity列
                query = (
                    session.query(string_info_model.timestamp, string_info_model.fixed_intensity)
                    .filter(string_info_model.device_id == device_id)
                    .filter(string_info_model.timestamp >= start_timestamp)
                    .filter(string_info_model.timestamp < end_timestamp)
                    .order_by(string_info_model.timestamp)
                    .all()
                )
                
                # 检查结果是否为空
                if not query:
                    return None
                
                # 检查是否所有值都不为None
                all_values_present = all(row.fixed_intensity is not None for row in query)
                if not all_values_present:
                    return None
                
                # 转换结果
                imputed_data = []
                for row in query:
                    # 转换时间戳为本地时间
                    local_time = datetime.fromtimestamp(row.timestamp).astimezone(timezone('Asia/Shanghai'))
                    imputed_data.append({
                        'date': local_time.strftime('%Y-%m-%d %H:%M:%S'),
                        'value': float(row.fixed_intensity)
                    })
                
                return imputed_data
            else:
                # 查询fixed_voltage列
                query = (
                    session.query(string_info_model.timestamp, string_info_model.fixed_voltage)
                    .filter(string_info_model.device_id == device_id)
                    .filter(string_info_model.timestamp >= start_timestamp)
                    .filter(string_info_model.timestamp < end_timestamp)
                    .order_by(string_info_model.timestamp)
                    .all()
                )
                
                # 检查结果是否为空
                if not query:
                    return None
                
                # 检查是否所有值都不为None
                all_values_present = all(row.fixed_voltage is not None for row in query)
                if not all_values_present:
                    return None
                
                # 转换结果
                imputed_data = []
                for row in query:
                    # 转换时间戳为本地时间
                    local_time = datetime.fromtimestamp(row.timestamp).astimezone(timezone('Asia/Shanghai'))
                    imputed_data.append({
                        'date': local_time.strftime('%Y-%m-%d %H:%M:%S'),
                        'value': float(row.fixed_voltage)
                    })
                
                return imputed_data
    
    except Exception as e:
        print(f"Error checking imputed data: {str(e)}")

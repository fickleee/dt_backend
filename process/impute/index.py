import pandas as pd
import os
from datetime import datetime, timedelta
import sqlite3
import time
from process.impute.utils import get_time_range, impute_and_fill_bulk
from pypots.imputation import SAITS, iTransformer, FreTS
# from model import impute # 测试用
# from utils import get_date_data # 测试用

import pytz

def get_station_info_orm(station, variable, start_time, end_time, repo_abs_path, database_manager=None, impute_model=None):
    # 定义上海时区
    shanghai_tz = pytz.timezone('Asia/Shanghai')
    
    # 将输入时间转换为上海时区的datetime对象
    start_datetime = pd.to_datetime(start_time).tz_localize(shanghai_tz)
    end_datetime = pd.to_datetime(end_time).tz_localize(shanghai_tz)
    
    # 转换时间格式为时间戳
    start_timestamp = int(start_datetime.timestamp())
    end_timestamp = int(end_datetime.timestamp())
    
    # 使用 ORM 中间件连接数据库
    db_name = f'{station}_impute'
    
    try:            
        with database_manager.get_session(db_name) as session:
            # 查询数据
            query = (
                session.query(impute_model)
                .filter(impute_model.timestamp >= start_timestamp)
                .filter(impute_model.timestamp < end_timestamp)
                .all()
            )
            if not query:
                return {'station_info': [], 'overview_res': []}
            
            # 转换结果为 DataFrame
            df = pd.DataFrame([{
                'timestamp': item.timestamp,
                'device_id': item.device_id,
                'error_count_intensity': item.error_count_intensity,
                'missing_count_intensity': item.missing_count_intensity,
                'error_count_voltage': item.error_count_voltage,
                'missing_count_voltage': item.missing_count_voltage
            } for item in query])

            # 计算overview_res
            df['date'] = pd.to_datetime(df['timestamp'], unit='s')
            
            # 根据variable选择对应的列名
            error_col = 'error_count_intensity' if variable == '0' else 'error_count_voltage'
            missing_col = 'missing_count_intensity' if variable == '0' else 'missing_count_voltage'
            
            # 聚合计算
            overview_df = df.groupby('date').agg({
                error_col: 'sum',
                missing_col: 'sum'
            }).reset_index()
            
            # 重命名列并转换日期格式
            overview_df.columns = ['date', 'error_value', 'missing_value']
            overview_df['date'] = overview_df['date'].dt.strftime('%Y-%m-%d %H:%M:%S')
            overview_res = overview_df.to_dict('records')


            # 计算station_info
            station_info = [{'name': station, 'key': station, 'err_rate': 0, 'missing_rate': 0, 'children': []}]
            box_dict = {}

            # 按设备号拆分并聚合
            df['box_id'], df['inverter_id'], df['string_id'] = zip(*df['device_id'].apply(lambda x: x.split('-')))
            for (box_id, inverter_id, string_id), group in df.groupby(['box_id', 'inverter_id', 'string_id']):
                # 根据变量选择错误和缺失计数
                if int(variable) == 0:  # 电流
                    error_count = group['error_count_intensity'].sum()
                    missing_count = group['missing_count_intensity'].sum()
                else:  # 电压
                    error_count = group['error_count_voltage'].sum()
                    missing_count = group['missing_count_voltage'].sum()

                # 计算组串的错误率和缺失率
                days = len(group['date'].unique())
                total_hours = days * 24
                err_rate = round((error_count / total_hours) * 100) if total_hours > 0 else 0
                missing_rate = round((missing_count / total_hours) * 100) if total_hours > 0 else 0

                # 更新箱变信息
                if box_id not in box_dict:
                    box_dict[box_id] = {'name': f'{box_id}号箱变', 'key': f'{station},{box_id}', 'err_rate': 0, 'missing_rate': 0, 'children': []}

                # 检查逆变器是否已存在
                inverter_info = next((inv for inv in box_dict[box_id]['children'] if inv['key'] == f'{station},{box_id},{inverter_id}'), None)
                if not inverter_info:
                    inverter_info = {'name': f'{inverter_id}号逆变器', 'key': f'{station},{box_id},{inverter_id}', 'err_rate': 0, 'missing_rate': 0, 'children': []}
                    box_dict[box_id]['children'].append(inverter_info)

                # 添加组串信息
                inverter_info['children'].append({
                    'name': f'{string_id}号组串',
                    'key': f'{station},{box_id},{inverter_id},{string_id}',
                    'err_rate': err_rate,
                    'missing_rate': missing_rate
                })

            # 计算逆变器和箱变的错误率和缺失率
            total_err_count = 0
            total_missing_count = 0
            total_count = 0

            for box_id, box_info in box_dict.items():
                box_err_count = 0
                box_missing_count = 0
                box_total_count = 0

                for inverter_info in box_info['children']:
                    inverter_err_count = 0
                    inverter_missing_count = 0
                    inverter_total_count = 0

                    for string_info in inverter_info['children']:
                        inverter_err_count += string_info['err_rate']
                        inverter_missing_count += string_info['missing_rate']
                        inverter_total_count += 1

                    if inverter_total_count > 0:
                        inverter_info['err_rate'] = round(inverter_err_count / inverter_total_count)
                        inverter_info['missing_rate'] = round(inverter_missing_count / inverter_total_count)

                    box_err_count += inverter_err_count
                    box_missing_count += inverter_missing_count
                    box_total_count += inverter_total_count

                if box_total_count > 0:
                    box_info['err_rate'] = round(box_err_count / box_total_count)
                    box_info['missing_rate'] = round(box_missing_count / box_total_count)

                station_info[0]['children'].append(box_info)

                total_err_count += box_err_count
                total_missing_count += box_missing_count
                total_count += box_total_count

            # 更新场站的错误率和缺失率
            if total_count > 0:
                station_info[0]['err_rate'] = round(total_err_count / total_count)
                station_info[0]['missing_rate'] = round(total_missing_count / total_count)

            return {'station_info': station_info, 'overview_res': overview_res}
            
    except Exception as e:
        print(f"Error getting station info for '{station}' using ORM: {e}")

def get_station_chart_orm(station_name, device_id, start_time, variable, repo_abs_path, database_manager=None, station_model=None):
    """
    使用 ORM 中间件获取站点图表数据
    
    参数：
    - station_name: 场站名称
    - device_id: 设备ID
    - start_time: 开始时间
    - variable: 变量类型，'0'表示电流，其他表示电压
    - repo_abs_path: 项目根目录绝对路径
    - database_manager: 数据库管理器实例
    - station_model: 场站表模型元组，包含 station_info, inverter_info, string_info
    
    返回：
    - 包含每个时间点数据的列表，每项包含date和value
    """
    
    # 定义上海时区
    shanghai_tz = pytz.timezone('Asia/Shanghai')
    
    # 将输入时间转换为上海时区的datetime对象
    start_datetime = pd.to_datetime(start_time).tz_localize(shanghai_tz)
    end_datetime = start_datetime + timedelta(hours=23, minutes=59, seconds=59)
    
    # 转换时间格式为时间戳
    start_timestamp = int(start_datetime.timestamp())
    end_timestamp = int(end_datetime.timestamp())
    
    
    try:
        # 获取字符串信息模型
        _, _, string_info_model = station_model
        
        # 使用 ORM 中间件连接数据库
        db_name = station_name
        
        with database_manager.get_session(db_name) as session:
            # 查询字符串信息
            query = (
                session.query(string_info_model.timestamp, string_info_model.intensity, string_info_model.voltage)
                .filter(string_info_model.timestamp >= start_timestamp)
                .filter(string_info_model.timestamp < end_timestamp)
                .filter(string_info_model.device_id == device_id)
                .order_by(string_info_model.timestamp)
                .all()
            )
            
            if not query:
                return []
            
            # 转换结果为 DataFrame
            df = pd.DataFrame([{
                'timestamp': item.timestamp,
                'intensity': item.intensity,
                'voltage': item.voltage
            } for item in query])
            
            if df.empty:
                return []
            
            # 转换时间戳为datetime
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s').dt.tz_localize('UTC').dt.tz_convert('Asia/Shanghai').dt.tz_localize(None)
            
            # 根据variable选择数据列
            if variable == '0':  # 电流数据
                value_col = 'intensity'
            else:  # 电压数据
                value_col = 'voltage'
            
            # 将数据转换为数值类型
            df[value_col] = pd.to_numeric(df[value_col], errors='coerce').fillna(0)
            
            # 生成结果
            res = []
            for _, row in df.iterrows():
                res.append({
                    'date': str(row['timestamp']),
                    'value': float(row[value_col])
                })
            
            return res
            
    except Exception as e:
        print(f"Error getting chart data for '{station_name}' using ORM: {e}")

def get_station_origin_data_orm_optimized(station_name, device_id, start_time, end_time, variable, repo_abs_path, database_manager=None, station_model=None, impute_model=None):
    """
    通过直接查询impute_model表获取统计数据
    
    参数：
    - station_name: 场站名称
    - device_id: 设备ID
    - start_time: 开始时间
    - end_time: 结束时间
    - variable: 变量类型，'0'表示电流，其他表示电压
    - repo_abs_path: 项目根目录绝对路径
    - database_manager: 数据库管理器实例
    - station_model: 场站表模型元组
    - impute_model: 填补数据模型
    
    返回：
    - 包含每日统计信息的列表，每项包含date, missing_count, error_count
    """
    # 定义上海时区
    shanghai_tz = pytz.timezone('Asia/Shanghai')
    # 将输入时间转换为上海时区的datetime对象
    start_datetime = pd.to_datetime(start_time).tz_localize(shanghai_tz)
    end_datetime = pd.to_datetime(end_time).tz_localize(shanghai_tz)
    
    # 转换时间格式为时间戳
    start_timestamp = int(start_datetime.timestamp())
    end_timestamp = int(end_datetime.timestamp())
    
    # 使用impute数据库
    db_name = f"{station_name}_impute"
    
    try:
        with database_manager.get_session(db_name) as session:
            # 直接查询impute_model表
            query = (
                session.query(impute_model)
                .filter(impute_model.timestamp >= start_timestamp)
                .filter(impute_model.timestamp < end_timestamp)
                .filter(impute_model.device_id == device_id)
                .all()
            )
            
            if not query:
                return []
            
            # 转换结果为所需格式
            res = []
            for item in query:
                # 将时间戳转换为日期字符串
                date_obj = datetime.fromtimestamp(item.timestamp, shanghai_tz).replace(tzinfo=None)
                date_str = str(date_obj.date())
                
                # 根据variable选择电流或电压的统计数据
                if variable == '0':  # 电流
                    missing_count = item.missing_count_intensity
                    error_count = item.error_count_intensity
                else:  # 电压
                    missing_count = item.missing_count_voltage
                    error_count = item.error_count_voltage
                
                res.append({
                    'date': date_str,
                    'missing_count': int(missing_count),
                    'error_count': int(error_count)
                })
            
            # 检查是否有日期缺失，如果有则补充空记录
            days = int((end_datetime - start_datetime).total_seconds() // (3600 * 24))
            existing_dates = {item['date'] for item in res}
            
            for i in range(days):
                day_start = start_datetime + timedelta(days=i)
                day_start_no_tz = day_start.replace(tzinfo=None)
                date_str = str(day_start_no_tz.date())
                
                if date_str not in existing_dates:
                    res.append({
                        'date': date_str,
                        'missing_count': 0,
                        'error_count': 0
                    })
            
            # 按日期排序
            res.sort(key=lambda x: x['date'])
            
            return res
            
    except Exception as e:
        print(f"Error getting data from impute model for '{station_name}': {e}")
        return []

def impute_schedule_bulk(process_date, station_name, repo_abs_path, database_manager, station_model, model_dict, impute_model, position=0):
    # 如果没有加载到任何模型，打印警告
    if not model_dict:
        print("警告: 未能加载任何模型，填补可能无法正常工作")

    start_timestamp, end_timestamp = get_time_range(process_date, 0)

    _,_, string_info_model = station_model

    impute_and_fill_bulk(station_name, database_manager, string_info_model, start_timestamp, end_timestamp, model_dict, impute_model, position)

def load_impute_models(repo_abs_path):
    """
    加载填补模型
    """
    model_dir_path = os.path.join(repo_abs_path, 'process', 'impute', 'model_multivariate')
    
    # 初始化模型字典
    model_dict = {}
    
    # 加载SAITS模型
    saits_path = os.path.join(model_dir_path, 'saits.pypots')
    if os.path.exists(saits_path):
        saits = SAITS(
            n_steps=24,
            n_features=18,  # 18个组串作为特征
            n_layers=2,
            d_model=256,
            d_ffn=128,
            n_heads=4,
            d_k=64,
            d_v=64,
            dropout=0.1,
        )
        try:
            saits.load(saits_path)
            model_dict['SAITS'] = saits
        except Exception as e:
            print(f"加载SAITS模型时出错: {e}")
    
    # 加载iTransformer模型
    itransformer_path = os.path.join(model_dir_path, 'itransformer.pypots')
    if os.path.exists(itransformer_path):
        itransformer = iTransformer(
            n_steps=24,
            n_features=18,  # 18个组串作为特征
            n_layers=2,
            d_model=256,
            d_ffn=128,
            n_heads=4,
            d_k=64,
            d_v=64,
            dropout=0.1,
        )
        try:
            itransformer.load(itransformer_path)
            model_dict['iTransformer'] = itransformer
        except Exception as e:
            print(f"加载iTransformer模型时出错: {e}")
    
    # 加载FreTS模型
    frets_path = os.path.join(model_dir_path, 'frets.pypots')
    if os.path.exists(frets_path):
        frets = FreTS(
            n_steps=24,
            n_features=18,  # 18个组串作为特征
            embed_size=256,
            hidden_size=256,
            channel_independence=False,
        )
        try:
            frets.load(frets_path)
            model_dict['FreTS'] = frets
        except Exception as e:
            print(f"加载FreTS模型时出错: {e}")

    return model_dict

if __name__ == '__main__':
    # start_timestamp = 1710691200
    # end_timestamp = 1710777599
    process_date = '2024-03-19'
    station_name = 'datu'
    repo_abs_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

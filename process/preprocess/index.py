import json
import os
import requests
import sqlite3 as sql
import pandas as pd
import re
from datetime import datetime, timedelta
import time
import pytz  # 引入pytz库来处理时区
import numpy as np
from sqlalchemy.exc import SQLAlchemyError
import logging

# 日志配置（只需在模块顶部配置一次即可）
logger = logging.getLogger(__name__)

# # 测试用
# from schema.session import DatabaseManager
# from schema.models import create_station_models, create_impute_model, create_user_model
# from dotenv import load_dotenv

# Preprocess的常量部分
#   在三张表的字段中，int类型字段
INT_COLUMN_LIST = [
    'sig_overvoltage',
    'sig_undervoltage',
    'sig_overfrequency',
    'sig_underfrequency',
    'sig_gridless',
    'sig_imbalance',
    'sig_overcurrent',
    'sig_midpoint_grounding',
    'sig_insulation_failure',
    'sig_excessive_DC',
    'sig_arc_self_protection',
    'sig_arc_failure',
]
#   获取前第 n 天的时间戳，默认为昨天
PREVIOUS_DAY = 1
#   查询的时间范围，默认为0时0分0秒0毫秒到23点59分59秒999毫秒
START_TIME_SCOPE = {
    'hour': 0,
    'minute': 0,
    'second': 0,
    'millisecond': 0
}
END_TIME_SCOPE = {
    'hour': 23,
    'minute': 59,
    'second': 59,
    'millisecond': 999
}
#   每个厂站的配置文件的名称，默认为 config.json
CONFIG_FILE_NAME = 'config.json'
#   kaiorsdb 查询时，聚合体的参数，默认对每小时的数据进行平均值计算，且时间戳为各小时的0分0秒
QUERY_AGGREGATORS = [
    {
        "name": "avg",
        "sampling": {
            "value": 1,
            "unit": "hours"
        },
        "align_start_time": True
    }
]
#   station_list对应的文件路径
STATION_LIST_PATH = {
    'first_dir': 'config',
    'name': 'station.json'
}

def get_yesterday_timestamp():  # 1.1
    # 设置时区为UTC+8（上海时区）
    shanghai_tz = pytz.timezone('Asia/Shanghai')

    # 获取当前日期，并设置为上海时区
    today = datetime.now(shanghai_tz)

    # 获取前一天的日期
    yesterday = today - timedelta(days=PREVIOUS_DAY)

    # 获取前一天0点的时间戳
    yesterday_start = yesterday.replace(hour=START_TIME_SCOPE['hour'], minute=START_TIME_SCOPE['minute'], second=START_TIME_SCOPE['second'], microsecond=START_TIME_SCOPE['millisecond'])
    yesterday_start_timestamp = int(yesterday_start.timestamp())

    # 获取前一天23点59分59秒999999毫秒的时间戳
    yesterday_end = yesterday.replace(hour=END_TIME_SCOPE['hour'], minute=END_TIME_SCOPE['minute'], second=END_TIME_SCOPE['second'], microsecond=END_TIME_SCOPE['millisecond'])
    yesterday_end_timestamp = int(yesterday_end.timestamp())

    # 获取前一天的日期字符串（格式为 "YYYY-MM-DD"）
    yesterday_date_str = yesterday.strftime('%Y-%m-%d')

    return yesterday_date_str, yesterday_start_timestamp, yesterday_end_timestamp
def get_anyday_timestamp(process_date): # process_date 形如"YYYY-MM-DD"
    # 解析输入的日期字符串为 datetime 对象
    date_obj = datetime.strptime(process_date, '%Y-%m-%d')

    # 设置上海时区
    shanghai_tz = pytz.timezone('Asia/Shanghai')
    date_obj = shanghai_tz.localize(date_obj)

    anyday_start_timestamp = int(
        time.mktime(date_obj.replace(hour=START_TIME_SCOPE['hour'], minute=START_TIME_SCOPE['minute'], second=START_TIME_SCOPE['second'], microsecond=START_TIME_SCOPE['millisecond']).timetuple()))

    anyday_end_timestamp = int(
        time.mktime(date_obj.replace(hour=END_TIME_SCOPE['hour'], minute=END_TIME_SCOPE['minute'], second=END_TIME_SCOPE['second'], microsecond=END_TIME_SCOPE['millisecond']).timetuple())
    )

    return anyday_start_timestamp, anyday_end_timestamp

def get_station_list(config_station_path):
    # 打开并读取配置文件
    with open(config_station_path, 'r', encoding='utf-8') as file:
        config_data = json.load(file)

    # 获取键为 'station_list' 对应的列表
    station_list = config_data.get('station_list', [])

    return station_list

def read_station_config(config_dir_path, station_name):
    # 获取配置文件路径
    config_station_path = os.path.join(config_dir_path, station_name, CONFIG_FILE_NAME)
    with open(config_station_path, 'r', encoding='utf-8') as file:
        config_data = json.load(file)
    return config_data

def process_config(config_data):
    config_mapping = dict()  # 以'tag_name'为键，'device_list'、'table'、'column'、'shared'为值
    shared_mapping = dict()  # 对于那些shared属性为1的'tag_name'，将其'{table}_{column}'作为键，空列表为默认值，并将'tag_name'加上到列表中
    query_metrics = []  # 查询请求体中的 "metrics"键对应的值

    config_metrics = config_data.get("metrics", [])
    for metric in config_metrics:
        config_mapping[metric.get("tag_name")] = {
            "device_list": metric.get("device_list"),
            "table": metric.get("table"),
            "column": metric.get("column"),
            "shared": metric.get("shared")
        }

        if metric.get("shared") == 1:
            shared_mapping.setdefault(f"{metric.get('table')} {metric.get('column')}", []).append(
                metric.get("tag_name"))

        metric_item = dict()
        metric_item["name"] = metric.get("tag_name")
        metric_item["tags"] = dict()
        metric_item["tags"]["project"] = metric.get("device_list")
        metric_item["aggregators"] = QUERY_AGGREGATORS
        metric_item["group_by"] = [
            {
                "name": "tag",
                "tags": ["project"]
            }
        ]
        query_metrics.append(metric_item)  # 完成查询请求体一个item的生成

    return config_mapping, shared_mapping, query_metrics


def query_remote_database(station_name, start_timestamp, end_timestamp,kairosdb_url,config_dir_path):
    # 1. 读取该厂站的配置文件
    config_data = read_station_config(config_dir_path, station_name)
    # 2. 生成查询与映射（映射可以方便后续的数据处理与本地数据库的存储）
    config_mapping, shared_mapping, query_metrics = process_config(config_data)
    query_body = {
        "start_absolute": start_timestamp,
        "end_absolute": end_timestamp,
        "metrics": query_metrics
    }
    # 3. 发送查询请求，增加超时和异常保护
    try:
        response = requests.post(
            kairosdb_url,
            headers={"Content-Type": "application/json"},
            data=json.dumps(query_body),
            timeout=600  # 设置超时时间（秒）
        )
        return response, config_mapping, shared_mapping
    except Exception as e:
        logger.error(f"查询KairosDB异常: {e}")
        # 构造一个假的 response 对象，status_code 非 200
        class DummyResponse:
            status_code = 500
            text = str(e)
            def json(self): return {}
        return DummyResponse(), config_mapping, shared_mapping

def transform_device_name(device_name, parts_num):
    sub_parts = device_name.split(':')

    if parts_num == 3:
        if len(sub_parts) < 3:
            raise ValueError("剩余字符串格式不正确，至少需要3个部分")
        part1, part2, part3 = sub_parts[-3], sub_parts[-2], sub_parts[-1]
        parts = [part1, part2, part3]
    elif parts_num == 2:
        if len(sub_parts) < 2:
            raise ValueError("剩余字符串格式不正确，至少需要2个部分")
        part2, part3 = sub_parts[-2], sub_parts[-1]
        parts = [part2, part3]
    else:
        raise ValueError("parts_num 必须为 2 或 3")

    # 从每个部分中提取出连续的数字部分
    def extract_number(part):
        match = re.search(r'\d+', part)
        if match:
            return match.group()
        return ""

    # 提取数字部分并去除前缀0
    numbers = [str(int(extract_number(part))) if extract_number(part) else "" for part in parts]

    # 使用"-"进行拼接
    device_name = "-".join(numbers)

    return device_name


def process_response(response, config_mapping, shared_mapping):
    is_valid_response = False
    irradiance_valid_column = 0
    if response.status_code == 200:
        data_dict = dict()

        query_results = response.json()
        for query in query_results['queries']:
            for result in query['results']:
                metric_name = result['name']
                tags = result['tags']
                values = result['values']

                # 检查 values 是否为空
                if not values:
                    logger.warning(f"No values found for metric '{metric_name}'. Skipping this result.")
                    continue

                is_valid_response = True

                table_column_str = get_table_column(metric_name, config_mapping)  # 形如"InverterInfo_intensity"
                if table_column_str not in data_dict:
                    data_dict[table_column_str] = {}

                device_name = 'None'
                for tag_key, tag_values in tags.items(): # tags形如{"tags": {"project": ["test-1", "test-2"]}}
                    device_name = tag_values[0]
                if config_mapping[metric_name]['table'] == 'StationInfo':
                    device_name = 'all'
                if config_mapping[metric_name]['table'] == 'StringInfo':
                    ## 老代码，无汇流箱对应的处理
                    # device_name = device_name + ':' + metric_name
                    # # device_name 形如DTZJJK:CDTGF:Q1:BT053:I004:PVINV_DCV28
                    # device_name = transform_device_name(device_name, 3)
                    
                    device_name = device_name + ':' + metric_name # device_name 形如DTZJJK:CDTGF:Q1:BT053:I004:PVINV_DCV28
                    # 特殊判断，使用正则匹配大涂剩余箱变中的特殊device_name
                    pattern = r':(\d+)HLX_(\d+)ZL_DC(?:I|PWR)'
                    if re.search(pattern, device_name):
                        matches = re.findall(pattern, device_name)
                        if matches:
                            inverter_str, string_str = matches[0]
                            box_pattern = r':BT(\d+):'
                            # 使用正则表达式提取box_id
                            box_match = re.search(box_pattern, device_name)
                            if box_match:
                                # 去除前缀0
                                box_id = str(int(box_match.group(1)))
                                inverter_str = str(int(inverter_str))
                                string_str = str(int(string_str))
                                device_name = f"{box_id}-{inverter_str}-{string_str}"
                    else:
                        # 对于其他情况，直接转换
                        device_name = transform_device_name(device_name, 3)
                elif config_mapping[metric_name]['table'] == 'InverterInfo':
                    device_name = transform_device_name(device_name, 2)

                # 生成24小时的时间戳列表
                start_time = datetime.fromtimestamp(values[0][0] / 1000).replace(minute=0, second=0, microsecond=0)
                timestamps = [int((start_time + timedelta(hours=i)).timestamp() * 1000) for i in range(24)]

                # 创建一个字典来存储查询结果
                # value_dict = {ts: val for ts, val in values}
                value_dict = {ts: {"is_valid": True, "value": val} for ts, val in values}
                # 确保每小时都有数据，没有数据的填充为0
                for ts in timestamps:
                    if ts not in value_dict:
                        value_dict[ts] = {"is_valid": False, "value": 0}

                if config_mapping[metric_name]['shared'] == 0:
                    data_dict[table_column_str][device_name] = value_dict  # 将指定列的某个设备的前一天24条数据加入到字典中
                elif device_name not in data_dict[table_column_str]:
                    irradiance_valid_column += 1
                    data_dict[table_column_str][device_name] = value_dict  # 对于share为1的列，如果是第一次添加，则将数据加入到字典中
                else:  # 对于share为1的列，如果之前已经添加过，则将value_dict中的值一一对应，与之前的数据进行add
                    irradiance_valid_column += 1
                    for ts_key, ts_value in value_dict.items():
                        # 先判断ts_key是否在data_dict中
                        if ts_key in data_dict[table_column_str][device_name]:
                            data_dict[table_column_str][device_name][ts_key]["value"] += ts_value["value"]
                        else:
                            logger.warning(f"Timestamp {ts_key} not found in existing data for {device_name} in {table_column_str}!")

        # 对于所有share为1的列，最终根据shared_mapping中指定表+列对应的值列表的长度，取平均值
        for shared_key, shared_value in shared_mapping.items():
            if shared_key in data_dict:
                for device_key, device_value in data_dict[shared_key].items():
                    for ts_key, ts_value in device_value.items():
                        if irradiance_valid_column != 0:
                            data_dict[shared_key][device_key][ts_key]["value"] = ts_value["value"] / irradiance_valid_column

        return data_dict,is_valid_response

    else:
        logger.error(f"查询失败，状态码：{response.status_code}")
        logger.error(f"响应内容：{response.text}")
        return None,is_valid_response


def transform_response2df(dict_response):
    dataframe_dict = {
        'StationInfo': [],
        'InverterInfo': {},
        'StringInfo': {}
    }

    processing_timestamps = []

    for table_column_str, device_data in dict_response.items():
        table_name, column_name = table_column_str.split(" ")

        if table_name == 'StationInfo':
            # 对于 StationInfo，我们期望所有数据都在 'all' 键下，并且只有24条记录。
            all_data = device_data.get('all', {})
            timestamps = list(all_data.keys())
            if len(processing_timestamps) == 0:
                processing_timestamps = [int(ts / 1000) for ts in timestamps]
            if len(timestamps) != 24:
                logger.warning(f"Expected 24 timestamps for {table_name}, but got {len(timestamps)}.")

            new_rows = [
                {'timestamp': ts, column_name: all_data[ts]["value"], 'is_valid': all_data[ts]["is_valid"]}
                for ts in timestamps
            ]
            dataframe_dict[table_name].extend(new_rows)

        elif table_name in ['InverterInfo', 'StringInfo']:
            for device_name, measurements in device_data.items():
                device_id = pcs_device_name(device_name)  # 将device_name转换成"001-001-001"的形式

                # 每个 device 应该有24条记录
                timestamps = list(measurements.keys())
                if len(processing_timestamps) == 0:
                    processing_timestamps = [int(ts / 1000) for ts in timestamps]
                if len(timestamps) != 24:
                    logger.warning(f"Expected 24 timestamps for device {device_name}, but got {len(timestamps)}.")

                if device_id not in dataframe_dict[table_name]:
                    dataframe_dict[table_name][device_id] = {}

                for ts in timestamps:
                    val_dict = measurements[ts]
                    if column_name in INT_COLUMN_LIST:
                        value = int(val_dict["value"])
                    else:
                        value = val_dict["value"]
                    if ts not in dataframe_dict[table_name][device_id]:
                        dataframe_dict[table_name][device_id][ts] = {}
                    dataframe_dict[table_name][device_id][ts][column_name] = value
                    # 如果 "is_valid" 字段已存在，则取交集
                    if "is_valid" in dataframe_dict[table_name][device_id][ts]:
                        dataframe_dict[table_name][device_id][ts]["is_valid"] &= val_dict["is_valid"]
                    else:
                        dataframe_dict[table_name][device_id][ts]["is_valid"] = val_dict["is_valid"]

    # Convert lists and dictionaries to DataFrames
    for table_name, data in dataframe_dict.items():
        if table_name == 'StationInfo':
            if data:
                dataframe_dict[table_name] = pd.DataFrame(data)
            else:
                dataframe_dict[table_name] = pd.DataFrame(columns=['timestamp'])
        else:
            combined_data = []
            for device_id, device_data in data.items():
                for ts, values in device_data.items():
                    row = {'timestamp': ts, 'device_id': device_id}
                    row.update(values)
                    combined_data.append(row)

            if combined_data:
                dataframe_dict[table_name] = pd.DataFrame(combined_data)
            else:
                dataframe_dict[table_name] = pd.DataFrame(columns=['timestamp', 'device_id'])


    return dataframe_dict, processing_timestamps

def pcs_device_name(device_name):
    # 分割 device_name 成各个部分
    device_info = device_name.split('-')

    # 对每个部分进行格式化，保证是三位数，不足的前面补0
    formatted_device_info = [part.zfill(3) for part in device_info]

    # 将格式化后的部分重新组合成完整的字符串
    device_id = '-'.join(formatted_device_info)

    return device_id

def get_table_column(metric_name, config_mapping):
    return f"{config_mapping[metric_name]['table']} {config_mapping[metric_name]['column']}"

def df2sqlite(dataframe_dict, station_name,local_database_path,processing_stamps):
    conn = sql.connect(local_database_path)
    cursor = conn.cursor()


    for table_name, df in dataframe_dict.items():
        current_table_name = f'{station_name}{table_name}'

        # 检查 DataFrame 是否为空
        if df.empty:
            print(f"警告: {current_table_name} 无数据。")
            continue  # 跳过当前循环，处理下一个 DataFrame

        df['timestamp'] = (df['timestamp'] / 1000).astype(int)
        if table_name == 'InverterInfo':
            # 分割 device_id 列，并扩展为多列
            device_info = df['device_id'].str.split('-', expand=True)
            df['box_id'] = device_info[0]
            df['inverter_id'] = device_info[1]

        elif table_name == 'StringInfo':
            # 分割 device_id 列，并扩展为多列
            device_info = df['device_id'].str.split('-', expand=True)
            df['box_id'] = device_info[0]
            df['inverter_id'] = device_info[1]
            df['string_id'] = device_info[2]
            df['fixed_intensity'] = None
            df['fixed_voltage'] = None

        # 检查并删除已存在的时间戳数据
        if processing_stamps:
            placeholders = ', '.join('?' for _ in processing_stamps)
            delete_query = f"""
            DELETE FROM {current_table_name}
            WHERE timestamp IN ({placeholders})
            """
            cursor.execute(delete_query, processing_stamps)

        df.to_sql(current_table_name, con=conn, index=False, if_exists='append')

    conn.close()

def df2orm(dataframe_dict, station_name, processing_stamps, database_manager, station_model):
    """
    使用 ORM 方式批量写入数据，支持事务回滚

    参数：
    - dataframe_dict: 包含 DataFrame 的字典 {表名: DataFrame}
    - station_name: 场站名称，用于确定数据库连接和表名
    - processing_stamps: 需要处理的 timestamp 列表（毫秒级）
    """

    # 获取对应模型类
    station_info, inverter_info, string_info = station_model

    session = None
    try:
        # 获取数据库会话
        session = database_manager.get_session(station_name)

        for table_name, df in dataframe_dict.items():
            if df.empty:
                logger.warning(f"\t{table_name} 无数据。")
                continue

            df['timestamp'] = (df['timestamp'] / 1000).astype(int)

            # 获取当前表模型
            Model = station_info
            if not Model:
                raise ValueError(f"未定义的表名: {table_name}")

            # 处理 device_id 字段（保持原逻辑）
            if table_name in ['InverterInfo', 'StringInfo']:
                device_info = df['device_id'].str.split('-', expand=True)
                if table_name == 'InverterInfo':
                    Model = inverter_info
                    df['box_id'] = device_info[0]
                    df['inverter_id'] = device_info[1]
                else: # table_name == 'StringInfo'
                    Model = string_info
                    df['box_id'] = device_info[0]
                    df['inverter_id'] = device_info[1]
                    df['string_id'] = device_info[2]

            # 删除已存在数据（事务1）
            if processing_stamps:
                delete_stmt = (
                    session.query(Model)
                    .filter(Model.timestamp.in_(processing_stamps))
                    .delete(synchronize_session=False)
                )
                logger.info(f"已删除 {delete_stmt} 条记录")

            # 批量插入（事务2）
            df = df.replace({np.nan: None}) # 新增：清洗所有NaN值（兼容所有数据库）
            records = df.to_dict('records')
            session.bulk_insert_mappings(Model, records)

        session.commit()

    except SQLAlchemyError as e:
        logger.error(f"数据库操作失败: {str(e)}")
        if session:
            session.rollback()
        raise
    finally:
        if session:
            session.close()

    logger.info(f"成功插入 {sum(len(df) for df in dataframe_dict.values())} 条记录")

def fill_voltage(df: pd.DataFrame) -> pd.DataFrame:
    """
    对datuStringInfo的DataFrame进行电压填补，仅填补voltage字段，其它字段保持原有值。
    假设每个box_id-inverter_id下string_id数量为偶数，前一半有数据，后一半无数据。
    填补规则：后一半string_id的voltage用前一半的voltage填补。
    """
    df_filled = df.copy()
    # 按变压器-逆变器分组
    for (box_id, inverter_id), group in df.groupby(['box_id', 'inverter_id']):
        string_ids = sorted(group['string_id'].unique())
        if len(string_ids) != 30:
            logger.warning(f"datu: box {box_id}, inverter {inverter_id} has {len(string_ids)} strings, expected 30. Skipping.")
            continue  # 跳过组串数不是30的组合

        half = len(string_ids) // 2
        existing_strings = string_ids[:half]
        missing_strings = string_ids[half:]

        # # 只处理后一半无数据的情况
        # if group[group['string_id'].isin(missing_strings)]['voltage'].notna().any():
        #     continue

        # 按照 timestamp 对齐填补
        for idx, src_str in enumerate(existing_strings):
            src_data = group[group['string_id'] == src_str].set_index('timestamp')
            # 目标string_id
            tgt_str1 = string_ids[2 * idx]
            tgt_str2 = string_ids[2 * idx + 1]
            for tgt_str in [tgt_str1, tgt_str2]:
                tgt_mask = (
                    (df_filled['box_id'] == box_id) &
                    (df_filled['inverter_id'] == inverter_id) &
                    (df_filled['string_id'] == tgt_str)
                )
                # 只填补voltage字段，按timestamp对齐
                for ts, voltage in src_data['voltage'].items():
                    row_mask = tgt_mask & (df_filled['timestamp'] == ts)
                    df_filled.loc[row_mask, 'voltage'] = voltage
    return df_filled

def get_basis_info(repo_abs_path):
    station_list_path = os.path.join(repo_abs_path, STATION_LIST_PATH['first_dir'], STATION_LIST_PATH['name'])
    yesterday_date, yesterday_start_timestamp, yesterday_end_timestamp = get_yesterday_timestamp()
    station_list = get_station_list(station_list_path)
    return yesterday_date, yesterday_start_timestamp, yesterday_end_timestamp, station_list

def get_basis_info_anyday(repo_abs_path,process_date):
    station_list_path = os.path.join(repo_abs_path, STATION_LIST_PATH['first_dir'], STATION_LIST_PATH['name'])
    station_list = get_station_list(station_list_path)
    anyday_start_timestamp, anyday_end_timestamp = get_anyday_timestamp(process_date)
    return anyday_start_timestamp, anyday_end_timestamp, station_list

def get_basis_info_manual(process_date):
    anyday_start_timestamp, anyday_end_timestamp = get_anyday_timestamp(process_date)
    return anyday_start_timestamp, anyday_end_timestamp

def preprocess_log(start_timestamp, end_timestamp, station_name,kairosdb_url, repo_abs_path, database_manager, station_model):
    config_dir_path = os.path.join(repo_abs_path, 'config')

    logger.info(f"{station_name}_preprocess started")
    logger.info(f"\t{station_name}_preprocess_step1: Query remote database")

    # Step 1: Query remote database
    default_response, config_mapping, shared_mapping = query_remote_database(station_name,
                                                                             start_timestamp * 1000,
                                                                             end_timestamp * 1000,
                                                                             kairosdb_url, config_dir_path)
    logger.info(f"\t{station_name}_preprocess_step2: Process response")
    # Step 2: Process response
    dict_response,is_valid_response = process_response(default_response, config_mapping, shared_mapping)
    if not is_valid_response:
        logger.warning("No valid data in {} station from {} to {}. preprocess_log function has stopped".format(station_name, start_timestamp, end_timestamp))
        return

    logger.info(f"\t{station_name}_preprocess_step3: Transform response to dataframe")
    # Step 3: Transform response to dataframe
    dataframe_dict, processing_stamps = transform_response2df(dict_response)
    logger.info(f"\t{station_name}_preprocess_step4: Save dataframe to sqlite")
    # Step 4: Save dataframe to sqlite
    df2orm(dataframe_dict, station_name, processing_stamps, database_manager, station_model)

    logger.info(f"{station_name}_preprocess completed")

def get_repo_abs_path():
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def check_timestamp_is_exist(start_timestamp, end_timestamp, station_name, repo_abs_path):
    database_path = os.path.join(repo_abs_path, 'database', '{}.db'.format(station_name))
    conn = sql.connect(database_path)
    cursor = conn.cursor()

    current_table_name = f'{station_name}StationInfo'
    
    # Query to check for any data within the timestamp range
    query = f"""
    SELECT EXISTS (
        SELECT 1
        FROM {current_table_name}
        WHERE timestamp BETWEEN ? AND ?
    )
    """
    
    cursor.execute(query, (start_timestamp, end_timestamp))
    result = cursor.fetchone()[0]
    
    # Close the connection
    conn.close()
    
    # Return True if data exists, False otherwise
    return result == 1

def check_history_timestamp_is_exist(start_timestamp, end_timestamp, station_name, repo_abs_path):
    database_path = os.path.join(repo_abs_path, 'database', '{}.db'.format(station_name))
    conn = sql.connect(database_path)
    cursor = conn.cursor()

    current_table_name = f'{station_name}StationInfo'
    
    # Query to check if there is at least one entry per day (30 days)，utc+8 timezone
    query_daily_data = f"""
    SELECT COUNT(DISTINCT DATE(timestamp + 28800, 'unixepoch')) as days_with_data
    FROM {current_table_name}
    WHERE timestamp BETWEEN ? AND ?
    """
    cursor.execute(query_daily_data, (start_timestamp, end_timestamp))
    days_with_data = cursor.fetchone()[0]
    # print("当前时间戳范围内包含的天数：", days_with_data)
    has_data_every_day = (days_with_data == 30)  # Expecting 30 days
    
    # Query to check if total entries match 24 * 30 (hourly data for 30 days)
    query_total_entries = f"""
    SELECT COUNT(*) as total_entries
    FROM {current_table_name}
    WHERE timestamp BETWEEN ? AND ?
    """
    cursor.execute(query_total_entries, (start_timestamp, end_timestamp))
    total_entries = cursor.fetchone()[0]
    has_full_data = (total_entries == 24 * 30)  # Expecting 720 entries
    # print("当前时间戳范围内包含的总条目数：", total_entries)
    
    # Close the connection
    conn.close()
    
    return has_data_every_day, has_full_data

if __name__ == '__main__':
    # # 获取当前环境变量，默认为development（通过在命令行中设置）
    # env_name = os.getenv("APP_ENV", "development").strip()
    # global_repo_abs_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))) # 本地测试用：最终的仓库名为 datangbackend
    # # 加载环境变量配置文件
    # load_dotenv(os.path.join(global_repo_abs_path, 'setting', f".env.{env_name}")) 
    # global_database_manager = DatabaseManager(global_repo_abs_path)
    # global_station_list = ['datu']
    # global_kairosdb_url = os.getenv('KAIROSDB_URL', 'http://localhost:8080/api/v1/datapoints/query').strip() # 获取KairosDB的URL
    # # 动态创建表
    # station_models = {station_name: create_station_models(station_name) for station_name in global_station_list} # 各场站的数据表模型
    # impute_models = {station_name: create_impute_model(station_name) for station_name in global_station_list} # 各场站的impute对应的表模型
    # user_model = create_user_model() # 用户表模型
    # print("当前开发模式：{} 数据库类型：{} 项目根目录：{} 场站列表：{}".format(env_name, os.getenv('DB_TYPE', 'sqlite').strip().lower(), global_repo_abs_path, global_station_list))

    # test_date_str = '2025-05-20'

    # start_timestamp, end_timestamp, station_list = get_basis_info_anyday(repo_abs_path=global_repo_abs_path,process_date=test_date_str)

    # for station_name in global_station_list:
    #     preprocess_log(start_timestamp, end_timestamp, station_name, global_kairosdb_url, global_repo_abs_path, global_database_manager, station_models[station_name])

    pass
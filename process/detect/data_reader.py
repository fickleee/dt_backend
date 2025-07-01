import os
import sqlite3
import pandas as pd
from sqlalchemy import or_
import logging

# 日志配置（只需在模块顶部配置一次即可）
logger = logging.getLogger(__name__)
 
def get_current_rad_df(repo_abs_path,station_name, history_timestamp_tuple, anomalous_ids, database_manager=None, station_model=None):
    current_df = get_current_df(repo_abs_path,station_name, history_timestamp_tuple, anomalous_ids)
    rad_df = get_rad_df(repo_abs_path,station_name, history_timestamp_tuple)
    return current_df, rad_df

def get_current_rad_df_orm(station_name, history_timestamp_tuple, anomalous_ids, database_manager=None, station_model=None):
    current_df = get_current_df_orm(station_name, history_timestamp_tuple, anomalous_ids, database_manager, station_model)
    rad_df = get_rad_df_orm(station_name, history_timestamp_tuple, database_manager, station_model)
    return current_df, rad_df

def get_current_df(repo_abs_path, station_name, history_timestamp_tuple, anomalous_ids):
    database_path = os.path.join(repo_abs_path, 'database', f'{station_name}.db')

    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    # 构建 WHERE 子句
    where_clauses = []
    params = []

    # 时间范围条件
    if history_timestamp_tuple:
        time_clauses = []
        for start_ts, end_ts in history_timestamp_tuple:
            time_clauses.append("(timestamp BETWEEN ? AND ?)")
            params.extend([start_ts, end_ts])
        where_clauses.append("(" + " OR ".join(time_clauses) + ")")

    # device_id 条件
    if anomalous_ids:
        id_placeholders = ",".join(["?"] * len(anomalous_ids))
        where_clauses.append(f"device_id IN ({id_placeholders})")
        params.extend(anomalous_ids)

    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)

    query_string_info = f'''
    SELECT 
        timestamp, 
        device_id,
        intensity 
    FROM {station_name}StringInfo
    {where_sql}
    '''

    query_string_info = f'''
    SELECT 
        timestamp, 
        device_id,
        intensity 
    FROM {station_name}StringInfo
    {where_sql}
    '''

    string_info_data = cursor.execute(query_string_info, params)
    string_info_rows = string_info_data.fetchall()
   
    cursor.close()
    conn.close()

    # process none value
    processed_rows = []
    for row in string_info_rows:
        processed_row = list(row)
        if processed_row[1] is None:
            processed_row[1] = 0.0
        processed_rows.append(tuple(processed_row))
    
    # turn processed_rows to dataframe
    current_df = pd.DataFrame(processed_rows, columns=['timestamp', 'device_id', 'intensity'])

    # turn column timestamp to datetime using utc8
    current_df['time'] = pd.to_datetime(current_df['timestamp'], unit='s') + pd.Timedelta(hours=8)

    # aggregate intensity hourly by device_id in avg function
    current_df.set_index('time', inplace=True)
    current_df = current_df.groupby('device_id').resample('h').agg({'intensity': 'mean'}).reset_index()

    print(f"电气量数据读取完成，当前数据包含 {len(current_df)} 条记录")
    return current_df

def get_current_df_orm(station_name, history_timestamp_tuple, anomalous_ids, database_manager=None, station_model=None):
    _, _, string_info = station_model

    try:
        with database_manager.get_session(station_name) as session:
            # 构建时间范围条件
            time_filters = []
            if history_timestamp_tuple:
                for start_ts, end_ts in history_timestamp_tuple:
                    time_filters.append(
                        (string_info.timestamp >= start_ts) & (string_info.timestamp <= end_ts)
                    )
            # 构建 device_id 条件
            query = session.query(
                string_info.timestamp,
                string_info.device_id,
                string_info.intensity
            )
            if time_filters:
                query = query.filter(or_(*time_filters))
            if anomalous_ids:
                query = query.filter(string_info.device_id.in_(anomalous_ids))

            rows = query.all()
            processed_rows = []
            for row in rows:
                row = list(row)
                if row[2] is None:
                    row[2] = 0.0
                processed_rows.append(tuple(row))

            current_df = pd.DataFrame(processed_rows, columns=['timestamp', 'device_id', 'intensity'])
            if current_df.empty:
                logger.info("Current(I) data reading completed, contains 0 records")
                return current_df

            current_df['time'] = pd.to_datetime(current_df['timestamp'], unit='s') + pd.Timedelta(hours=8)
            current_df.set_index('time', inplace=True)
            current_df = current_df.groupby('device_id').resample('h').agg({'intensity': 'mean'}).reset_index()
            logger.info(f"Current(I) data reading completed, contains {len(current_df)} records")
            return current_df
    except Exception as e:
        logger.error(f"Error in get_current_df_orm: {e}")
        return pd.DataFrame(columns=['timestamp', 'device_id', 'intensity', 'time'])

def get_rad_df(repo_abs_path, station_name, history_timestamp_tuple=None):
    database_path = os.path.join(repo_abs_path, 'database', f'{station_name}.db')

    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    # 构建 WHERE 子句
    where_clauses = []
    params = []

    # 时间范围条件
    if history_timestamp_tuple:
        time_clauses = []
        for start_ts, end_ts in history_timestamp_tuple:
            time_clauses.append("(timestamp BETWEEN ? AND ?)")
            params.extend([start_ts, end_ts])
        where_clauses.append("(" + " OR ".join(time_clauses) + ")")

    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)

    query_string_info = f'''
    SELECT 
        timestamp, 
        irradiance
    FROM {station_name}StationInfo
    {where_sql}
    '''

    string_info_data = cursor.execute(query_string_info, params)
    string_info_rows = string_info_data.fetchall()
   
    cursor.close()
    conn.close()

    # process none value
    processed_rows = []
    for row in string_info_rows:
        processed_row = list(row)
        if processed_row[1] is None:
            processed_row[1] = 0.0
        processed_rows.append(tuple(processed_row))
    
    # turn processed_rows to dataframe
    rad_df = pd.DataFrame(processed_rows, columns=['timestamp', 'irradiance'])

    # turn column timestamp to datetime using utc8
    rad_df['time'] = pd.to_datetime(rad_df['timestamp'], unit='s') + pd.Timedelta(hours=8)

    # aggregate irradiance hourly in avg function
    rad_df.set_index('time', inplace=True)
    rad_df = rad_df.resample('h').agg({'irradiance': 'mean'}).reset_index()

    print(f"辐照数据读取完成，当前数据包含 {len(rad_df)} 条记录")
    return rad_df

def get_rad_df_orm(station_name, history_timestamp_tuple=None, database_manager=None, station_model=None):
    station_info, _, _ = station_model

    try:
        with database_manager.get_session(station_name) as session:
            # 构建时间范围条件
            time_filters = []
            if history_timestamp_tuple:
                for start_ts, end_ts in history_timestamp_tuple:
                    time_filters.append(
                        (station_info.timestamp >= start_ts) & (station_info.timestamp <= end_ts)
                    )
            query = session.query(
                station_info.timestamp,
                station_info.irradiance
            )
            if time_filters:
                query = query.filter(or_(*time_filters))

            rows = query.all()
            processed_rows = []
            for row in rows:
                row = list(row)
                if row[1] is None:
                    row[1] = 0.0
                processed_rows.append(tuple(row))

            rad_df = pd.DataFrame(processed_rows, columns=['timestamp', 'irradiance'])
            if rad_df.empty:
                logger.info("Radiation data reading completed, current data contains 0 records")
                return rad_df

            rad_df['time'] = pd.to_datetime(rad_df['timestamp'], unit='s') + pd.Timedelta(hours=8)
            rad_df.set_index('time', inplace=True)
            rad_df = rad_df.resample('h').agg({'irradiance': 'mean'}).reset_index()
            logger.info(f"Radiation data reading completed, current data contains {len(rad_df)} records")
            return rad_df
    except Exception as e:
        logger.error(f"Error in get_rad_df_orm: {e}")
        return pd.DataFrame(columns=['timestamp', 'irradiance', 'time'])
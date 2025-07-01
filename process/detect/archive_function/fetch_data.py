import os 
import sys
# top_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
# sys.path.insert(0, top_path)

import sqlite3
from datetime import datetime 
import time 

import pandas as pd

# from constants import PROC_DB_NAME, PROC_TIME_WINDOW
PROC_DB_NAME = "datu.db"
PROC_TIME_WINDOW = 120

END_DATE = "2024-09-30"
import re

DB_NAME = "datu.db"
TIME_WINDOW = 30


########## utils
def date_to_stamp(t_date: str) ->int: 
    t_stamp = int(time.mktime(datetime.strptime(t_date+
        ' 23:59:59','%Y-%m-%d %H:%M:%S').timetuple()))
    
    return t_stamp

def stamp_to_date(t_stamp: int) ->str:
    t_date = datetime.fromtimestamp(t_stamp).strftime('%Y-%m-%d %H:%M:%S')
    
    return t_date

######### fetch data
def get_station_data(station_name, end_date, batch_size=1000000):
    """
    分批从数据库中获取数据，避免内存溢出
    返回格式：
    {
        'box_id-inv_id': DataFrame(
            columns=['time', 'PV1输入电流', 'PV2输入电流', ...],
            data=[...]
        ),
        ...
    }
    """
    # 计算时间范围
    end_date = pd.to_datetime(end_date)
    start_date = end_date - pd.Timedelta(days=TIME_WINDOW-1)
    end_timestamp = int(end_date.timestamp())
    start_timestamp = int(start_date.timestamp())
    
    current_dir = os.path.dirname(os.path.abspath(__file__))
    root_dir = os.path.abspath(os.path.join(current_dir, '..', '..'))
    conn = sqlite3.connect(os.path.join(root_dir, 'database', DB_NAME))

    time_query = f'''
    SELECT DISTINCT timestamp,
            datetime(timestamp, 'unixepoch', 'localtime') as formatted_time
    FROM {station_name}StringInfo 
    WHERE timestamp >= ? AND timestamp < ?
    ORDER BY timestamp
    '''
    timestamps = pd.read_sql_query(time_query, conn, params=(start_timestamp, end_timestamp))
    
    data_dict = {}
    total_timestamps = len(timestamps)

    def clean_current(x):
        """处理电流值：空值、空字符串转为0"""
        if pd.isna(x) or x == '' or x is None:
            return 0
        try:
            return float(x)
        except (ValueError, TypeError):
            return 0
    
    for i in range(0, total_timestamps, batch_size):
        batch_timestamps = timestamps['timestamp'][i:i + batch_size].tolist()
        if not batch_timestamps:
            continue
            
        # print(f"处理批次 {i//batch_size + 1}/{(total_timestamps + batch_size - 1)//batch_size}")
        
        timestamp_params = ','.join('?' * len(batch_timestamps))
        
        query = f'''
        SELECT datetime(timestamp, 'unixepoch', 'localtime') as time,
                string_id, inverter_id, box_id, 
                CAST(fixed_intensity AS FLOAT) as fixed_intensity, 
                CAST(intensity AS FLOAT) as intensity 
        FROM {station_name}StringInfo
        WHERE timestamp IN ({timestamp_params})
        ORDER BY timestamp
        '''
        
        df_batch = pd.read_sql_query(query, conn, params=batch_timestamps)
        if df_batch.empty:
            continue
        
        # 确保数值类型正确
        df_batch['fixed_intensity'] = df_batch['fixed_intensity'].apply(clean_current)
        df_batch['intensity'] = df_batch['intensity'].apply(clean_current)
        
        # 只在fixed_intensity不为0时使用fixed_intensity，否则使用intensity
        df_batch['current'] = df_batch.apply(
            lambda row: row['fixed_intensity'] if row['fixed_intensity'] > 0 else row['intensity'],
            axis=1
        )
        
        # 按箱变和逆变器分组处理
        for (box_id, inverter_id), group in df_batch.groupby(['box_id', 'inverter_id']):
            key = f"{box_id}-{inverter_id}"
            group = group.drop_duplicates(['time', 'string_id'])
            pivot_df = pd.pivot(
                group,
                index='time',
                columns='string_id',
                values='current'
            ).reset_index()

            pivot_df.columns.name = None
            current_cols = [col for col in pivot_df.columns if col != 'time']
            pivot_df.rename(columns={col: f'PV{int(col)}输入电流' for col in current_cols}, inplace=True)

            current_cols = [col for col in pivot_df.columns if col != 'time']
            pivot_df = pivot_df[['time'] + sorted(current_cols, key=lambda x: int(re.findall(r'\d+', x)[0]))]

            if key in data_dict:
                data_dict[key] = pd.concat([data_dict[key], pivot_df])
            else:
                data_dict[key] = pivot_df

    for key in data_dict:
        data_dict[key] = data_dict[key].sort_values('time').drop_duplicates('time', keep='first')
    
    conn.close()
    return data_dict
        


def get_env_data(station_name, end_date):
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        root_dir = os.path.abspath(os.path.join(current_dir, '..', '..'))
        conn = sqlite3.connect(os.path.join(root_dir, 'database', DB_NAME))
        cursor = conn.cursor()

        end_timestamp = date_to_stamp(end_date)
        start_timestamp = end_timestamp - (TIME_WINDOW * 24 * 60 * 60)

        query = f'''
            SELECT timestamp, irradiance
            FROM {station_name}StationInfo 
            WHERE timestamp BETWEEN ? AND ?
            '''
        db_rows = cursor.execute(query, (start_timestamp, end_timestamp)).fetchall()
        env_data = {
            "time": [],
            "rad": []
        }
        
        for row in db_rows:
            timestamp, rad = row 
            formatted_time = stamp_to_date(timestamp)
            env_data["time"].append(formatted_time)
            env_data["rad"].append(rad)

        cursor.close()
        conn.close()

        env_data = pd.DataFrame(env_data)
        env_data["time"] = pd.to_datetime(env_data["time"])
        env_data.set_index("time", inplace=True)

        return env_data
    except Exception as e:
        raise f"Error in process for getting station string data: {str(e)}"

if __name__ == '__main__':
    station_name = 'datu'
    repo_abs_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    start_window_timestamp = 1717171200
    end_window_timestamp = 1719676800
    get_station_data(station_name, repo_abs_path,start_window_timestamp,end_window_timestamp)

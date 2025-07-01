import sqlite3
import time
from datetime import datetime, timedelta
import logging

# 日志配置（只需在模块顶部配置一次即可）
logger = logging.getLogger(__name__)

def read_data(station_name, update_time, database_path):
    """从数据库读取指定时间范围的数据"""
    # 原有的 read_data 函数逻辑
    end_time = update_time
    end_time = end_time.replace(hour=23, minute=59, second=59)
    start_time = update_time - timedelta(days=29)
    start_time = start_time.replace(hour=0, minute=0, second=0)

    start_date = int(time.mktime(start_time.timetuple()))
    end_date = int(time.mktime(end_time.timetuple()))

    conn = sqlite3.connect(database_path)
    cursor = conn.cursor()

    query_string_info = f'''
    SELECT 
        timestamp, 
        string_id, 
        inverter_id, 
        box_id, 
        intensity 
    FROM {station_name}StringInfo
    WHERE timestamp BETWEEN ? AND ?
    '''
    
    string_info_data = cursor.execute(query_string_info, (start_date, end_date))
    string_info_rows = string_info_data.fetchall()
   
    cursor.close()
    conn.close()
   
    # 处理空值
    processed_rows = []
    for row in string_info_rows:
        processed_row = list(row)
        if processed_row[4] is None:
            processed_row[4] = 0.0
        processed_rows.append(tuple(processed_row))

    # 检查数据完整性
    if len(processed_rows) == 0:
        return [], False

    timestamps = [datetime.fromtimestamp(row[0]) for row in processed_rows]
    min_timestamp = min(timestamps)
    max_timestamp = max(timestamps)

    if (max_timestamp - min_timestamp) >= timedelta(days=29):
        return processed_rows, True
    else:
        return processed_rows, False
    
def read_data_orm(station_name, update_time, database_manager=None, station_model=None):
    """
    使用SQLAlchemy ORM从数据库读取指定时间范围的数据
    """
    _, _, string_info = station_model

    end_time = update_time.replace(hour=23, minute=59, second=59)
    start_time = update_time - timedelta(days=29)
    start_time = start_time.replace(hour=0, minute=0, second=0)

    start_date = int(time.mktime(start_time.timetuple()))
    end_date = int(time.mktime(end_time.timetuple()))

    try:
        with database_manager.get_session(station_name) as session:
            rows = (
                session.query(
                    string_info.timestamp,
                    string_info.string_id,
                    string_info.inverter_id,
                    string_info.box_id,
                    string_info.intensity
                )
                .filter(string_info.timestamp >= start_date)
                .filter(string_info.timestamp <= end_date)
                .all()
            )

            # 处理空值
            processed_rows = []
            for row in rows:
                # row: (timestamp, string_id, inverter_id, box_id, intensity)
                row = list(row)
                if row[4] is None:
                    row[4] = 0.0
                processed_rows.append(tuple(row))

            if len(processed_rows) == 0:
                return [], False

            timestamps = [datetime.fromtimestamp(r[0]) for r in processed_rows]
            min_timestamp = min(timestamps)
            max_timestamp = max(timestamps)

            if (max_timestamp - min_timestamp) >= timedelta(days=29):
                return processed_rows, True
            else:
                return processed_rows, False

    except Exception as e:
        logger.error(f"Error reading data from database: {e}")
        return [], False

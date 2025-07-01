import sqlite3
from datetime import datetime
import time
import os

def get_history_current_test():
    # Get the current directory path
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # 数据库连接
    conn = sqlite3.connect(os.path.join(current_dir, 'database', 'datang.db'))


    station_name = "datu"
    # 指定的时间范围
    start_date = int(time.mktime(datetime.strptime('2024-10-31 00:00:00', '%Y-%m-%d %H:%M:%S').timetuple()))
    end_date = int(time.mktime(datetime.strptime('2024-10-31 02:00:00', '%Y-%m-%d %H:%M:%S').timetuple()))

    box_number = "001"
    inverter_number = "001"

    cursor = conn.cursor()
    # Query string info table
    query = f'''
    SELECT timestamp, string_id, fixed_intensity
    FROM {station_name}StringInfo 
    WHERE box_id = ? AND inverter_id = ?
    AND timestamp BETWEEN ? AND ?
    ORDER BY timestamp ASC
    '''

    string_info_rows = cursor.execute(query, (box_number, inverter_number, start_date, end_date)).fetchall()

    # Create a dictionary to organize data by timestamp
    result = []
    temp_dict = {}  # Initialize empty dictionary

    for row in string_info_rows:
        timestamp, string_id, intensity = row
        # 将时间戳转换为datetime格式
        formatted_time = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
        # 将string_id转换为"PV{n}输入电流"格式
        string_key = f"PV{int(string_id)}输入电流"
        
        if temp_dict == {}:
            temp_dict['时间'] = formatted_time
            temp_dict[string_key] = intensity
        elif formatted_time != temp_dict['时间']:
            result.append(temp_dict)
            temp_dict = {}
            temp_dict['时间'] = formatted_time
            temp_dict[string_key] = intensity
        else:
            temp_dict[string_key] = intensity
    result.append(temp_dict)

    # Print the result to verify
    for entry in result:
        print(entry)

    cursor.close()
    conn.close()

def get_history_irradiance_test():
    # group_name here is actually the inverter_name, for example:"DTZJJK-CDTGF-Q1-BT001-I001"
    try:
        # Get the current directory path
        current_dir = os.path.dirname(os.path.abspath(__file__))

        # 数据库连接
        conn = sqlite3.connect(os.path.join(current_dir, 'database', 'datang.db'))

        cursor = conn.cursor()

        station_name = "datu"
        # 指定的时间范围
        start_timestamp = int(time.mktime(datetime.strptime('2024-10-30 00:00:00', '%Y-%m-%d %H:%M:%S').timetuple()))
        end_timestamp = int(time.mktime(datetime.strptime('2024-10-31 02:00:00', '%Y-%m-%d %H:%M:%S').timetuple()))


        # Query string info table
        query = f'''
        SELECT timestamp, irradiance
        FROM {station_name}StationInfo 
        WHERE timestamp BETWEEN ? AND ?
        ORDER BY timestamp ASC
        '''

        string_info_rows = cursor.execute(query, (start_timestamp, end_timestamp)).fetchall()

        # Create a dictionary to organize data by timestamp
        result = []
        temp_dict = {}  # Initialize empty dictionary

        for row in string_info_rows:
            timestamp, irradiance = row
            # 将时间戳转换为datetime格式
            formatted_time = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
            temp_dict['Time'] = formatted_time
            temp_dict['rad'] = irradiance
            result.append(temp_dict)
            temp_dict = {}
        # checkpoint
        for entry in result:
            print(entry)

        cursor.close()
        conn.close()

        return result, 200

    except Exception as e:
        print(f"Error getting history irradiance: {str(e)}")
        return [], 500

if __name__ == "__main__":
    #get_history_irradiance_test()
    get_history_current_test()

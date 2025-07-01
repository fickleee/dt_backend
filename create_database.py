import sqlite3
import os

# Get the current directory path
current_dir = os.path.dirname(os.path.abspath(__file__))

# Create the database directory if it doesn't exist
os.makedirs(os.path.join(current_dir, 'database'), exist_ok=True)

# Connect to SQLite database
conn = sqlite3.connect(os.path.join(current_dir, 'database', 'datang.db'))
cursor = conn.cursor()

# 场站列表
station_list = ["datu", "tangjing", "tangyun", "eryuan", "wushashan", "wanshi", "daxue"]

# 创建UserInfo表
cursor.execute('''
CREATE TABLE IF NOT EXISTS UserInfo (
    user_name TEXT NOT NULL PRIMARY_KEY,
    user_type TEXT NOT NULL,
    user_password TEXT NOT NULL,
    user_email TEXT NOT NULL,
    user_phone TEXT NOT NULL
)
''')

# 对于每个场站，创建StringInfo, InverterInfo, StationInfo表
for station in station_list:
    station_id = station.lower().replace(' ', '_')  # 将场站名称转换为有效的表名

    # 创建StringInfo表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS {}StringInfo (
        timestamp DATETIME NOT NULL,
        device_id TEXT NOT NULL,
        string_id TEXT NOT NULL,
        inverter_id TEXT NOT NULL,
        box_id TEXT NOT NULL,
        intensity REAL,
        voltage REAL,
        fixed_intensity REAL,
        fixed_voltage REAL,
        PRIMARY KEY (timestamp, device_id)
    )
    '''.format(station_id))

    # 创建InverterInfo表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS {}InverterInfo (
        timestamp DATETIME NOT NULL,
        device_id TEXT NOT NULL,
        inverter_id TEXT NOT NULL,
        box_id TEXT NOT NULL,
        intensity REAL,
        voltage REAL,
        power REAL,
        generated_energy REAL,
        temperature REAL,
        sig_overvoltage INTEGER,
        sig_undervoltage INTEGER,
        sig_overfrequency INTEGER,
        sig_underfrequency INTEGER,
        sig_gridless INTEGER,
        sig_imbalance INTEGER,
        sig_overcurrent INTEGER,
        sig_midpoint_grounding INTEGER,
        sig_insulation_failure INTEGER,
        sig_excessive_DC INTEGER,
        sig_arc_self_protection INTEGER,
        sig_arc_failure INTEGER,
        PRIMARY KEY (timestamp, device_id)
    )
    '''.format(station_id))

    # 创建StationInfo表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS {}StationInfo (
        timestamp DATETIME NOT NULL,
        irradiance REAL,
        temperature REAL,
        power REAL,
        PRIMARY KEY (timestamp)
    )
    '''.format(station_id))

    # 创建StringOverview表
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS {}StringOverview (
        timestamp DATETIME NOT NULL,
        device_id TEXT NOT NULL,
        error_count_intensity INTEGER NOT NULL,
        missing_count_intensity INTEGER NOT NULL,
        error_count_voltage INTEGER NOT NULL,
        missing_count_voltage INTEGER NOT NULL,
        PRIMARY KEY (timestamp, device_id)
    )
    '''.format(station_id))

# 提交事务
conn.commit()

# 关闭Cursor和Connection
cursor.close()
conn.close()
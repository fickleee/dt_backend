import sqlite3
import os

# Get the current directory path
current_dir = os.path.dirname(os.path.abspath(__file__))

# Create the database directory if it doesn't exist
os.makedirs(os.path.join(current_dir, 'database'), exist_ok=True)

# 场站列表
station_list = ["datu", "tangjing", "tangyun", "eryuan", "wushashan", "daxue", "mayu", "fuyang"]

for station_id in station_list:
    # Connect to SQLite database
    conn = sqlite3.connect(os.path.join(current_dir, 'database', '{}.db'.format(station_id)))
    cursor = conn.cursor()

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
            is_valid INTEGER DEFAULT 1,
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
            sum_energy REAL,
            month_energy REAL,
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
            sig_temperature INTEGER,
            is_valid INTEGER DEFAULT 1,
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
            is_valid INTEGER DEFAULT 1,
            PRIMARY KEY (timestamp)
        )
        '''.format(station_id))

    # 提交事务
    conn.commit()

    # 关闭Cursor和Connection
    cursor.close()
    conn.close()

for station_id in station_list:

    # Connect to SQLite database
    conn = sqlite3.connect(os.path.join(current_dir, 'database', '{}_impute.db'.format(station_id)))
    cursor = conn.cursor()

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
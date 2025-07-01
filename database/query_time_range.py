import sqlite3
import datetime

# 连接到数据库
conn = sqlite3.connect('datu.db')
cursor = conn.cursor()

# 查询 datuInverterInfo 表的时间范围
cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM datuInverterInfo")
inverter_min, inverter_max = cursor.fetchone()
print("datuInverterInfo 表时间范围:")
print(f"最早时间: {datetime.datetime.fromtimestamp(inverter_min).strftime('%Y-%m-%d %H:%M:%S')}")
print(f"最晚时间: {datetime.datetime.fromtimestamp(inverter_max).strftime('%Y-%m-%d %H:%M:%S')}")

# 查询 datuStationInfo 表的时间范围
cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM datuStationInfo")
station_min, station_max = cursor.fetchone()
print("\ndatuStationInfo 表时间范围:")
print(f"最早时间: {datetime.datetime.fromtimestamp(station_min).strftime('%Y-%m-%d %H:%M:%S')}")
print(f"最晚时间: {datetime.datetime.fromtimestamp(station_max).strftime('%Y-%m-%d %H:%M:%S')}")

# 查询 datuStringInfo 表的时间范围
cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM datuStringInfo")
string_min, string_max = cursor.fetchone()
print("\ndatuStringInfo 表时间范围:")
print(f"最早时间: {datetime.datetime.fromtimestamp(string_min).strftime('%Y-%m-%d %H:%M:%S')}")
print(f"最晚时间: {datetime.datetime.fromtimestamp(string_max).strftime('%Y-%m-%d %H:%M:%S')}")

# 关闭连接
conn.close() 
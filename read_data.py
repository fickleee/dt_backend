import sqlite3
from datetime import datetime
import time
import os
# Get the current directory path
current_dir = os.path.dirname(os.path.abspath(__file__))

# 数据库连接
conn = sqlite3.connect(os.path.join(current_dir, 'database', 'datang.db'))

invertercursor = conn.cursor()

station_name = "datu"
# 指定的时间范围
start_date = int(time.mktime(datetime.strptime('2024-10-31 00:00:00', '%Y-%m-%d %H:%M:%S').timetuple()))
end_date = int(time.mktime(datetime.strptime('2024-10-31 01:00:00', '%Y-%m-%d %H:%M:%S').timetuple()))

# 查询InverterInfo表
query_inverter_info = '''
SELECT * FROM {}InverterInfo
WHERE timestamp between ? and ?
'''.format(station_name.lower().replace(' ', '_'))
inverter_info_data = invertercursor.execute(query_inverter_info,(start_date, end_date))
inverter_info_rows = inverter_info_data.fetchall()
# 打印结果
print("InverterInfo Data:")
print(len(inverter_info_rows))
for row in inverter_info_rows:
    print(row)

invertercursor.close()
cursor = conn.cursor()
# 查询StringInfo表
query_string_info = f'''
SELECT * FROM {station_name}StringInfo
WHERE timestamp between ? and ?
'''

# 执行查询
string_info_data = cursor.execute(query_string_info,(start_date, end_date))
string_info_rows = string_info_data.fetchall()
print("\nStringInfo Data:")
print(len(string_info_rows))
for row in string_info_rows:
    print(row[0])

# 关闭Cursor和Connection
cursor.close()
conn.close()
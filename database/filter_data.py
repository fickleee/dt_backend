import sqlite3
import datetime
import time

# 定义时间范围
start_date = datetime.datetime(2025, 3, 20, 0, 0, 0)
end_date = datetime.datetime(2025, 4, 2, 23, 59, 59)

# 转换为Unix时间戳
start_timestamp = int(start_date.timestamp())
end_timestamp = int(end_date.timestamp())

print(f"处理时间范围: {start_date.strftime('%Y-%m-%d %H:%M:%S')} 到 {end_date.strftime('%Y-%m-%d %H:%M:%S')}")
print(f"对应时间戳: {start_timestamp} 到 {end_timestamp}")

# 连接到原始数据库
conn = sqlite3.connect('datu.db')
cursor = conn.cursor()

# 创建新数据库
new_conn = sqlite3.connect('datu_filtered.db')
new_cursor = new_conn.cursor()

# 获取表结构并在新数据库中创建相同的表
print("\n正在创建表结构...")

# 处理 datuInverterInfo 表
cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='datuInverterInfo'")
create_table_sql = cursor.fetchone()[0]
new_cursor.execute(create_table_sql)

# 处理 datuStationInfo 表
cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='datuStationInfo'")
create_table_sql = cursor.fetchone()[0]
new_cursor.execute(create_table_sql)

# 处理 datuStringInfo 表
cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='datuStringInfo'")
create_table_sql = cursor.fetchone()[0]
new_cursor.execute(create_table_sql)

# 提交表结构更改
new_conn.commit()

# 复制符合时间范围的数据
print("\n开始复制数据...")

# 处理 datuInverterInfo 表
print("处理 datuInverterInfo 表...")
cursor.execute("SELECT COUNT(*) FROM datuInverterInfo WHERE timestamp >= ? AND timestamp <= ?", 
               (start_timestamp, end_timestamp))
total_rows = cursor.fetchone()[0]
print(f"找到 {total_rows} 条记录")

if total_rows > 0:
    cursor.execute("SELECT * FROM datuInverterInfo WHERE timestamp >= ? AND timestamp <= ?", 
                   (start_timestamp, end_timestamp))
    
    # 获取列名
    column_names = [description[0] for description in cursor.description]
    placeholders = ', '.join(['?'] * len(column_names))
    columns = ', '.join(column_names)
    
    # 批量插入数据
    batch_size = 10000
    count = 0
    
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        
        new_cursor.executemany(f"INSERT INTO datuInverterInfo ({columns}) VALUES ({placeholders})", rows)
        count += len(rows)
        print(f"已处理 {count}/{total_rows} 条记录")
        
        # 每批次提交一次
        new_conn.commit()

# 处理 datuStationInfo 表
print("\n处理 datuStationInfo 表...")
cursor.execute("SELECT COUNT(*) FROM datuStationInfo WHERE timestamp >= ? AND timestamp <= ?", 
               (start_timestamp, end_timestamp))
total_rows = cursor.fetchone()[0]
print(f"找到 {total_rows} 条记录")

if total_rows > 0:
    cursor.execute("SELECT * FROM datuStationInfo WHERE timestamp >= ? AND timestamp <= ?", 
                   (start_timestamp, end_timestamp))
    
    # 获取列名
    column_names = [description[0] for description in cursor.description]
    placeholders = ', '.join(['?'] * len(column_names))
    columns = ', '.join(column_names)
    
    # 批量插入数据
    batch_size = 10000
    count = 0
    
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        
        new_cursor.executemany(f"INSERT INTO datuStationInfo ({columns}) VALUES ({placeholders})", rows)
        count += len(rows)
        print(f"已处理 {count}/{total_rows} 条记录")
        
        # 每批次提交一次
        new_conn.commit()

# 处理 datuStringInfo 表
print("\n处理 datuStringInfo 表...")
cursor.execute("SELECT COUNT(*) FROM datuStringInfo WHERE timestamp >= ? AND timestamp <= ?", 
               (start_timestamp, end_timestamp))
total_rows = cursor.fetchone()[0]
print(f"找到 {total_rows} 条记录")

if total_rows > 0:
    cursor.execute("SELECT * FROM datuStringInfo WHERE timestamp >= ? AND timestamp <= ?", 
                   (start_timestamp, end_timestamp))
    
    # 获取列名
    column_names = [description[0] for description in cursor.description]
    placeholders = ', '.join(['?'] * len(column_names))
    columns = ', '.join(column_names)
    
    # 批量插入数据
    batch_size = 10000
    count = 0
    
    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        
        new_cursor.executemany(f"INSERT INTO datuStringInfo ({columns}) VALUES ({placeholders})", rows)
        count += len(rows)
        print(f"已处理 {count}/{total_rows} 条记录")
        
        # 每批次提交一次
        new_conn.commit()

# 关闭连接
cursor.close()
conn.close()
new_cursor.close()
new_conn.close()

print("\n数据处理完成！")
print(f"新数据库已保存为: datu_filtered.db")

# 验证新数据库中的数据
print("\n验证新数据库中的数据...")
verify_conn = sqlite3.connect('datu_filtered.db')
verify_cursor = verify_conn.cursor()

# 验证 datuInverterInfo 表
verify_cursor.execute("SELECT MIN(timestamp), MAX(timestamp), COUNT(*) FROM datuInverterInfo")
min_ts, max_ts, count = verify_cursor.fetchone()
print("datuInverterInfo 表:")
if min_ts is not None:
    print(f"时间范围: {datetime.datetime.fromtimestamp(min_ts).strftime('%Y-%m-%d %H:%M:%S')} 到 {datetime.datetime.fromtimestamp(max_ts).strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"记录数: {count}")
else:
    print("表为空")

# 验证 datuStationInfo 表
verify_cursor.execute("SELECT MIN(timestamp), MAX(timestamp), COUNT(*) FROM datuStationInfo")
min_ts, max_ts, count = verify_cursor.fetchone()
print("\ndatuStationInfo 表:")
if min_ts is not None:
    print(f"时间范围: {datetime.datetime.fromtimestamp(min_ts).strftime('%Y-%m-%d %H:%M:%S')} 到 {datetime.datetime.fromtimestamp(max_ts).strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"记录数: {count}")
else:
    print("表为空")

# 验证 datuStringInfo 表
verify_cursor.execute("SELECT MIN(timestamp), MAX(timestamp), COUNT(*) FROM datuStringInfo")
min_ts, max_ts, count = verify_cursor.fetchone()
print("\ndatuStringInfo 表:")
if min_ts is not None:
    print(f"时间范围: {datetime.datetime.fromtimestamp(min_ts).strftime('%Y-%m-%d %H:%M:%S')} 到 {datetime.datetime.fromtimestamp(max_ts).strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"记录数: {count}")
else:
    print("表为空")

verify_cursor.close()
verify_conn.close() 
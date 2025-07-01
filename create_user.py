import sqlite3

# 连接到SQLite数据库
# 如果文件不存在，会自动在当前目录创建一个数据库文件
conn = sqlite3.connect('./database/user.db')
cursor = conn.cursor()

# 场站列表
station_list = ["datu", "tangjing", "tangyun", "eryuan", "wushashan", "wanshi", "daxue"]

# 创建UserInfo表
cursor.execute('''
CREATE TABLE IF NOT EXISTS UserInfo (
    user_name TEXT NOT NULL PRIMARY KEY,
    user_type TEXT NOT NULL,
    user_password TEXT NOT NULL,
    user_email TEXT NOT NULL,
    user_phone TEXT NOT NULL
)
''')
cursor.execute("ALTER TABLE UserInfo ADD COLUMN user_validated BOOLEAN NOT NULL DEFAULT 0")
# 提交事务
conn.commit()

# 关闭Cursor和Connection
cursor.close()
conn.close()


#===============写入admin==============

import hashlib

# 假设tmp是你要插入的MD5加密字符串
tmp = "AdminPassword"

# 将字符串转换为MD5加密格式
def md5_encrypt(password):
    return hashlib.md5(password.encode()).hexdigest()


conn = sqlite3.connect('./database/user.db')
cursor = conn.cursor()
# 准备SQL插入语句
sql = '''INSERT INTO UserInfo (
    user_name, user_type, user_password, user_email, user_phone, user_validated
) VALUES (?, ?, ?, ?, ?, ?)'''

# 执行SQL语句
try:
    # 将明文密码转换为MD5加密字符串
    user_password = md5_encrypt(tmp)
    
    # 插入数据
    cursor.execute(sql, ('admin', 'admin', user_password, '995696521@qq.com', '17326082289', 1))
    
    # 提交事务
    conn.commit()
    print("数据插入成功")
except sqlite3.Error as e:
    print(f"数据库错误: {e}")
except Exception as e:
    print(f"错误: {e}")
finally:
    # 关闭Cursor和Connection:
    if conn:
        cursor.close()
        conn.close()
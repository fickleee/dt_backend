import os
import openpyxl
import datetime
import time
import sqlite3

# 数据库连接
conn = sqlite3.connect('./database/datang.db')
cursor = conn.cursor()

cursor.execute("ALTER TABLE UserInfo ADD COLUMN user_validated BOOLEAN NOT NULL DEFAULT 0")


conn.commit()

# 关闭Cursor和Connection
cursor.close()
conn.close()
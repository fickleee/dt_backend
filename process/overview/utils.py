from datetime import datetime, timedelta
import time
import pytz  # 引入pytz库来处理时区
import json
import requests
import logging
# 日志配置（只需在模块顶部配置一次即可）
logger = logging.getLogger(__name__)


def get_time_range(process_date, previous_day=30):
    # 解析输入的日期字符串为 datetime 对象
    end_date_obj = datetime.strptime(process_date, '%Y-%m-%d')

    # 设置上海时区
    shanghai_tz = pytz.timezone('Asia/Shanghai')
    end_date_obj = shanghai_tz.localize(end_date_obj)

    start_date_obj = end_date_obj - timedelta(days=previous_day)

    start_timestamp = int(time.mktime(start_date_obj.replace(hour=0, minute=0, second=0, microsecond=0).timetuple()))
    end_timestamp = int(time.mktime(end_date_obj.replace(hour=23, minute=59, second=59, microsecond=999).timetuple()))

    return start_timestamp, end_timestamp

# 获取 token
def get_token(username, password):
    url = f"http://api1.zklf-tech.com/api/auth/oauth/token?username={username}&password={password}&grant_type=password&client_id=client-app&client_secret=123456&systemType=INSPECTION_SYSTEM&loginType=2&domain=yweos.zklf-tech.com"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        res = response.json()
        token = res["data"]["tokenHead"] + res["data"]["token"]
        return token
    except requests.exceptions.RequestException as e:
        logger.error(f"网络请求失败: {e}")
        return None

import datetime
import pytz

def date2timestamp(process_date):
    """
    将日期字符串转换为当天的起止时间戳（毫秒）
    """
    tz = pytz.timezone('Asia/Shanghai')
    start_dt = tz.localize(datetime.datetime.strptime(process_date, "%Y-%m-%d"))
    end_dt = start_dt + datetime.timedelta(days=1) - datetime.timedelta(seconds=1)
    start_timestamp = int(start_dt.timestamp())
    end_timestamp = int(end_dt.timestamp())
    return start_timestamp, end_timestamp

def normalize(val, border):
    """
    归一化
    """
    return (val - border['min']) / (border['max'] - border['min'])

def denormalize(val, border):
    """
    反归一化
    """
    return val * (border['max'] - border['min']) + border['min']
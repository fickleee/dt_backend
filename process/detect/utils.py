from datetime import datetime, timedelta
import time
import json
import json
import pytz  # 引入pytz库来处理时区

def get_time_range(process_date, previous_day=0):
    # 解析输入的日期字符串为 datetime 对象
    end_date_obj = datetime.strptime(process_date, '%Y-%m-%d')

    # 设置上海时区
    shanghai_tz = pytz.timezone('Asia/Shanghai')
    end_date_obj = shanghai_tz.localize(end_date_obj)

    start_date_obj = end_date_obj - timedelta(days=previous_day)

    start_timestamp = int(time.mktime(start_date_obj.replace(hour=0, minute=0, second=0, microsecond=0).timetuple()))
    end_timestamp = int(time.mktime(end_date_obj.replace(hour=23, minute=59, second=59, microsecond=999).timetuple()))

    return start_timestamp, end_timestamp

def get_history_timestamp(process_date, time_window=30):
    # 假设process_date形如 "2024-01-30"，我需要获取其历史 time_window 天的的0点时间戳和该日期的23:59:59时间戳；以及1年前这个日期范围的时间戳和2年前这个日期范围的时间戳
    start_timestamp, end_timestamp = get_time_range(process_date, previous_day=time_window)
    one_year_ago = (datetime.strptime(process_date, '%Y-%m-%d') - timedelta(days=365)).strftime('%Y-%m-%d')
    two_years_ago = (datetime.strptime(process_date, '%Y-%m-%d') - timedelta(days=730)).strftime('%Y-%m-%d')
    one_year_ago_start, one_year_ago_end = get_time_range(one_year_ago, previous_day=time_window)
    two_years_ago_start, two_years_ago_end = get_time_range(two_years_ago, previous_day=time_window)
    return (two_years_ago_start, two_years_ago_end), (one_year_ago_start, one_year_ago_end), (start_timestamp, end_timestamp)

def get_anomalous_string_ids(result_path):
    with open(result_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    results = data.get('results', {})
    anomalous_ids = [
        sid for sid, info in results.items()
        if info.get('diagnosis_results') and len(info.get('diagnosis_results')) > 0
    ]
    return anomalous_ids


def update_degradation_scores(string_id, degradation_score,result_path):
    with open(result_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    results = data.get('results', {})
    results[string_id]['degradation_score'] = degradation_score
    with open(result_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def update_degradation_scores_dict(degradation_dict, result_path):
    with open(result_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    results = data.get('results', {})
    for key, value in degradation_dict.items():
        if key in results:
            results[key]['degradation_score'] = value

    data['results'] = results
    # 将更新后的数据写回到文件  
    with open(result_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False)







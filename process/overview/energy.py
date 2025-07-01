from sqlalchemy.exc import SQLAlchemyError
from process.overview.utils import get_time_range
import json
import requests
import logging

# 日志配置（只需在模块顶部配置一次即可）
logger = logging.getLogger(__name__)

COST_ENERGY_POINTS_DICT = {
    "daxue": "DTZJJK:DXGF:Q1:AGC:YC00004",
    "eryuan": "DTZJJK:EYZGF:Q1:AGC:YC00007",
    "fuyang": "DTZJJK:FYGF:Q1:ZZXT:YC00898",
    "mayu": "DTZJJK:MYGF:Q1:ZZXT:YM00009",
    "tangjing": "DTZJJK:TJGF:Q1:ZZXT:YM00021",
    "tangyun": "DTZJJK:TYGF:Q1:AGC:YC00003",
    "wushashan": "DTZJJK:WSSCQGF:Q1:ZZXT:YC00598",
    "datu": "DTZJJK:CDTGF:Q1:AVC:YC00025"
}

PLAN_ENERGY_POINTS_DICT = {
    "daxue": "DTZJJK:DXGF:Q1:annualGoalM_C",
    "eryuan": "DTZJJK:EYZGF:Q1:annualGoalM_C",
    "fuyang": "DTZJJK:FYGF:Q1:annualGoalM_C",
    "mayu": "DTZJJK:MYGF:Q1:annualGoalM_C",
    "tangjing": "DTZJJK:TJGF:Q1:annualGoalM_C",
    "tangyun": "DTZJJK:TYGF:Q1:annualGoalM_C",
    "wushashan": "DTZJJK:WSSCQGF:Q1:annualGoalM_C",
    "datu": "DTZJJK:CDTGF:Q1:annualGoalM_C"
}

COST_QUERY_AGGREGATORS = [
    {
        "name": "sum",
        "sampling": {
            "value": 1,
            "unit": "days"
        },
        "align_start_time": True
    }
]

PLAN_QUERY_AGGREGATORS = [
    {
        "name": "first",
        "sampling": {
            "value": 1,
            "unit": "months"
        },
        "align_start_time": True
    }
]

def query_template(start_timestamp, end_timestamp, kairosdb_url, test_point, query_aggregators):
    query_metrics = []

    metric_item = dict()
    tag_name = test_point.split(":")[-1]
    device_list = [test_point.rsplit(':', 1)[0]] # 设备列表，通常只有一个设备
    metric_item["name"] = tag_name
    metric_item["tags"] = dict()
    metric_item["tags"]["project"] = device_list
    metric_item["aggregators"] = query_aggregators
    metric_item["group_by"] = [
        {
            "name": "tag",
            "tags": ["project"]
        }
    ]

    query_metrics.append(metric_item)

    query_body = {
        "start_absolute": start_timestamp,
        "end_absolute": end_timestamp,
        "metrics": query_metrics
    }

    try:
        response = requests.post(
            kairosdb_url,
            headers={"Content-Type": "application/json"},
            data=json.dumps(query_body),
            timeout=500  # 可根据实际情况调整超时时间
        )
        if response.status_code == 200:
            query_results = response.json()
            for query in query_results['queries']:
                for result in query['results']:
                    metric_name = result['name']
                    tags = result['tags']
                    values = result['values']

                    # 检查 values 是否为空
                    if not values:
                        logger.warning(f"No values found for metric '{metric_name}'. default set to 0.")
                        return 0

                    if query_aggregators[0]['name'] == 'first': # 表示计划发电量
                        return values[0][1] if values else 0
                    else: # 表示有功消耗，统计
                        total_value = sum(value[1] for value in values if value[1] is not None)
                        return total_value
        else:
            logger.error(f"Failed to query energy: {response.status_code} - {response.text}")
            return 0
    except Exception as e:
        logger.error(f"Exception when querying KairosDB: {e}")
        return 0

def query_plan_energy(process_date, kairosdb_url, station_name):
    test_point = PLAN_ENERGY_POINTS_DICT.get(station_name)

    start_timestamp, end_timestamp = get_time_range(process_date, previous_day=31)
    start_timestamp *= 1000  # 转换为毫秒
    end_timestamp *= 1000  # 转换为毫秒

    plan_energy = query_template(
        start_timestamp,
        end_timestamp,
        kairosdb_url,
        test_point,
        PLAN_QUERY_AGGREGATORS
    )

    return plan_energy

def query_generation(start_timestamp, end_timestamp, database_manager, station_model, station_name):
    # 获取对应模型类
    _, inverter_info_model, _ = station_model

    total_generated_energy = 0
    total_sum_energy = 0
    total_month_energy = 0

    session = None
    try:
        session = database_manager.get_session(station_name)

        from sqlalchemy import func

        # 查询每个 device_id 在时间范围内的最大 generated_energy、sum_energy、month_energy
        results = (
            session.query(
                inverter_info_model.device_id,
                func.max(inverter_info_model.generated_energy).label("max_generated_energy"),
                func.max(inverter_info_model.sum_energy).label("max_sum_energy"),
                func.max(inverter_info_model.month_energy).label("max_month_energy"),
            )
            .filter(inverter_info_model.timestamp >= start_timestamp)
            .filter(inverter_info_model.timestamp <= end_timestamp)
            .group_by(inverter_info_model.device_id)
            .all()
        )

        # 分别求和
        total_generated_energy = sum(r.max_generated_energy or 0 for r in results)
        total_sum_energy = sum(r.max_sum_energy or 0 for r in results)
        total_month_energy = sum(r.max_month_energy or 0 for r in results)

        return total_generated_energy, total_sum_energy, total_month_energy

    except SQLAlchemyError as e:
        print(f"数据库操作失败: {str(e)}")
        if session:
            session.rollback()
        raise
    finally:
        if session:
            session.close()
        return total_generated_energy, total_sum_energy, total_month_energy
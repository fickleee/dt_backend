import json
import requests
import os
from datetime import datetime, timedelta

STATION2SOLAR_NAME = {
    'datu': '大唐长大涂光伏电站',
    'wushashan': '大唐乌沙山厂区光伏电站',
    'eryuan': '大唐文成县二源光伏电站',
    'tangyun': '浙江大唐唐云光伏电站',
    'tangjing': '唐景光伏电站',
    'daxue': '大峃光伏电站',
    'fuyang': '大唐万市光伏电站',
    'mayu': '唐屿光伏电站'
}

# 查询某电站的计划，并获取最新的计划与planCode
def get_station_plan(token, solar_name):
    query_url = 'http://api1.zklf-tech.com/api/inspection/inspection/planBase/pagePlanInfo'
    headers = {
        "Content-Type": "application/json",
        'Authorization': token
    }

    # 动态计算时间范围
    end_time = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    start_time = end_time - timedelta(days=185)  # 跨越至少一个季度

    # 请求参数 - 根据接口描述构建JSON数据
    payload = {
        "page": {
            "pageNum": 1,
            "pageSize": 10
        },
        "params": {
            "queryKey": solar_name,
            "startTime": start_time.strftime("%Y-%m-%d %H:%M:%S"),
            "endTime": end_time.strftime("%Y-%m-%d %H:%M:%S"),
            "inspectionScene": 1
        }
    }

    # 发送POST请求
    response = requests.post(
        query_url,
        headers=headers,
        data=json.dumps(payload)  # 将字典转换为JSON字符串
    )
    station_plan_list = json.loads(response.text).get('data', {}).get("list", [])
    # 获取最新的计划，planCode与 planArea
    latest_plan = station_plan_list[0] if station_plan_list else None
    lastest_plan_code = ""
    lastest_plan_area_list = []
    if latest_plan:
        lastest_plan_code = latest_plan.get("planCode", "")
        lastest_plan_area_list = latest_plan.get("planArea", "").split(",") if latest_plan.get("planArea") else []

    return lastest_plan_code, lastest_plan_area_list

def get_all_zone_faults(token, plan_code, zone_id):
    query_url = 'http://api1.zklf-tech.com/api/inspection/inspection/solarFault/queryPage'
    headers = {
        "Content-Type": "application/json",
        'Authorization': token
    }
    page_num = 1
    page_size = 50  # 可以适当调大，提高效率
    all_faults = []

    while True:
        payload = {
            "page": {
                "pageNum": page_num,
                "pageSize": page_size
            },
            "params": {
                "planCode": plan_code,
                "zoneId": zone_id
            }
        }
        response = requests.post(
            query_url,
            headers=headers,
            data=json.dumps(payload)
        )
        res = json.loads(response.text)
        data_list = res.get("data", {}).get("list", [])
        if not data_list:
            break
        all_faults.extend(data_list)
        if len(data_list) < page_size:
            break
        page_num += 1

    return all_faults

def faults2dict(faults):
    fault_dict = {}
    for fault in faults:
        panel_name = fault.get("panelName", "")
        if panel_name not in fault_dict:
            fault_dict[panel_name] = []

        current_fault = dict()
        current_fault['task_id'] = fault.get("taskId", "")
        current_fault['zone_id'] = fault.get("zoneId", "")
        current_fault['panel_gps'] = fault.get("panelGps", "")
        current_fault['ir_image'] = fault.get("irImage", "")
        current_fault['ir_coord'] = fault.get("irCoord", "")
        current_fault['ir_resolution_ratio'] = fault.get("irResolutionRatio", "")
        current_fault['rgb_image'] = fault.get("rgbImage", "")
        current_fault['rgb_coord'] = fault.get("rgbCoord", "")
        current_fault['rgb_resolution_ratio'] = fault.get("rgbResolutionRatio", "")
        current_fault['fault_type'] = fault.get("faultType", "")
        current_fault['fault_info'] = fault.get("faultInfo", "")
        current_fault['u_time'] = fault.get("uTime", "")
        current_fault['c_time'] = fault.get("cTime", "")
        fault_dict[panel_name].append(current_fault)
    return fault_dict

def save_faults2json(faults_dict, station_name, process_date, repo_abs_path):
    dir_path = os.path.join(repo_abs_path, "data", station_name, "reports")
    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    json_file_path = os.path.join(dir_path, f"{process_date}.json")
    with open(json_file_path, "w", encoding="utf-8") as json_file:
        json.dump(faults_dict, json_file, ensure_ascii=False)

def export_report(repo_abs_path, station_name, process_date, token):
    solar_name = STATION2SOLAR_NAME.get(station_name, station_name)
    lastest_plan_code, lastest_plan_area_list = get_station_plan(token, solar_name)

    all_faults_list = []
    for zone_id in lastest_plan_area_list:
        faults = get_all_zone_faults(token, lastest_plan_code, zone_id)
        all_faults_list.extend(faults)
    faults_dict = faults2dict(all_faults_list)
    save_faults2json(faults_dict, station_name, process_date, repo_abs_path)

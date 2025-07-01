import json
import os
import re
import time
import sqlite3
from datetime import datetime, timedelta 
def get_station_diagnosis(station_name,date,sample_factor, sample_size):
    print(station_name,date,sample_factor, sample_size)
    sample_factor='original'
    sample_size='day'
    results_json = get_json(station_name, date,'results')
    station_result=[]
    if results_json:
        station_result.append(get_diagnosis_results_bystation(results_json,station_name))
    return station_result
    

def get_json(station_name, date,folder_option):
    results_json={}
    station_results_folder ='./data/'+station_name+'/'+folder_option+'/'
    # print(station_results_folder)
    if not os.path.exists(station_results_folder):
        print(f"'{station_results_folder}' does not exist.")
        return
    json_file_path = station_results_folder+date+'.json'
    if not os.path.exists(json_file_path):
        print(f"'{date}' {folder_option} does not exist.")
        target_date = datetime.strptime(date, '%Y-%m-%d')
        # print("target_date",target_date)
        found_file = False  
        while not found_file:
            # 边界检查
            if target_date <= datetime(1, 1, 1):
                print("Reached minimum date limit, stopping the search.")
                return
            target_date -= timedelta(days=1) 
            json_file_path = station_results_folder + target_date.strftime('%Y-%m-%d') + '.json'
            if os.path.exists(json_file_path):
                print(f"before target_date found {target_date.strftime('%Y-%m-%d')}.")
                found_file = True
                break
    with open(json_file_path, 'r', encoding='utf-8') as json_file:
        results_json= json.load(json_file)
        # print(f"Data from '{json_file_path}':", results_json)
        return results_json


def get_diagnosis_results_bystation(results_json,station_name):
    station_diagnosis_results = {
        "anomaly_type": "",
        "key": station_name,
        "name": station_name,
        "children": []
    }
    final_children = station_diagnosis_results["children"]
    for key, value in results_json["results"].items():
        box_id = value["box_id"]
        inverter_id = value["inverter_id"]
        string_id = value["string_id"]
        box_name=f"{box_id}号箱变"
        inverter_name=f"{inverter_id}号逆变器"
        string_name=f"{string_id}号组串电流"
       
        box_obj = next((item for item in final_children if item["name"] == box_name), None)
        if not box_obj:
            box_key=f"{station_name},{box_id}"
            box_obj = {"name": box_name, "key":box_key,"children": [],"anomaly_type": ''}
            final_children.append(box_obj)
        
       
        inverter_obj = next((item for item in box_obj["children"] if item["name"] ==inverter_name), None)
        if not inverter_obj:
            inverter_key=f"{box_key},{inverter_id}"
            inverter_obj = {"name": inverter_name,"key":inverter_key, "children": [],"anomaly_type": ''}
            box_obj["children"].append(inverter_obj)
        
        if "diagnosis_results" in value and len(value["diagnosis_results"]) > 0:
            first_diagnosis_result = value["diagnosis_results"][0]
            anomaly_type = f"{first_diagnosis_result['result']} {first_diagnosis_result['rate'] * 100:.0f}%"  
        else:
            # 处理没有诊断结果的情况
            first_diagnosis_result = {}  # 
            anomaly_type=''
        # first_diagnosis_result = value["diagnosis_results"][0] 
        # print("111",first_diagnosis_result,"222",value)
        
        string_obj = next((item for item in inverter_obj["children"] if item["name"] == string_name), None)
        if not string_obj:
            string_key=f"{inverter_key},{string_id}号组串电流"
            string_obj = {"name": string_name, "key":string_key,"anomaly_type": anomaly_type}
            inverter_obj["children"].append(string_obj)
    
    # print(station_diagnosis_results)
    return station_diagnosis_results


def get_string_diagnosis(station_name, date,box_id,inverter_id,string_id):
    print(station_name, date,box_id,inverter_id,string_id)
    results_json = get_json(station_name, date,'results')
    connect_id=f"{box_id}-{inverter_id}-{string_id}"
    time_series_result_location= {
            "location_id": '',  
            "time_series_result": {}
        }
    report_result={
        "faultType": '',
        "faultInfo":'',
        "high": '',
        "panelName":'',
        "averageTemperature": '',
        "temperatureDifference": '',     
        "taskId": '',
        "zoneId": '',
        "GPS":'',
        "irImage": '',
        "rgbImage": '',
    }
    if results_json:
        time_series_result_location=get_diagnosis_result_bystring(results_json,connect_id) 
    else:
        print(f"{date} no results_json")
        
    time_series_result=time_series_result_location["time_series_result"]
    location_id=time_series_result_location["location_id"]

    if location_id:
        reports_json = get_json(station_name, date,'reports')
        if results_json:
            report_result=get_diagnosis_report_bystring(reports_json,location_id)
        else:
            print(f"{date}no reports_json") 
    else:
        print("no location_id can not find report")

    string_data=get_string_data_fromdb(station_name,date,box_id,inverter_id,string_id)
    string_timestamp_arr=string_data["string_timestamp_arr"]
    string_data_arr=string_data["string_data_arr"]
    string_assist_data_arr=string_data["string_assist_data"]
    
    # print(report_result,time_series_result,string_timestamp_arr,string_data_arr)
    return {
        "report_result":report_result,
        "time_series_result":time_series_result,
        "timestamp":string_timestamp_arr,
        "data":string_data_arr,
        "assistdata":string_assist_data_arr
    }


def get_diagnosis_result_bystring(results_json,connect_id):
    
    string_result_obj = results_json["results"].get(connect_id)
    # print(string_result_obj)
    if string_result_obj:
        time_series_result = {}  
        diagnosis_results=string_result_obj["diagnosis_results"]
        for index, result in enumerate(diagnosis_results):
            formatted_result = f"{result['result']} {result['rate'] * 100:.0f}%"
            if index == 0:
                time_series_result["1"] = formatted_result
            elif index == 1:
                time_series_result["2"] = formatted_result
            elif index == 2:
                time_series_result["3"] = formatted_result
            elif index == 3:
                time_series_result["4"] = formatted_result
            elif index == 4:
                time_series_result["5"] = formatted_result
        # print(time_series_result)
       
        location_id = string_result_obj.get('location_id', '')  
        return {
            "location_id": location_id,
            "time_series_result": time_series_result
        }
    else:
        print("No string_result_obj")
        return {
            "location_id": '',  
            "time_series_result": {}
        }


def get_diagnosis_report_bystring(reports_json, location_id):
    found_flag=False
    for obj in reports_json["data"]["list"]:
        if obj["panelName"] == location_id:
            found_flag=True
            string_report_obj=obj
            # fault_reason=f"{string_report_obj['faultType']}:{string_report_obj['faultInfo']}"
           # 解析 panelGps 字符串为 Python 对象
            panel_gps = json.loads(string_report_obj["panelGps"])
           
            # 获取第一个坐标
            first_coordinate = panel_gps[0] if panel_gps else None
            gps_string = f"{first_coordinate['lng']}, {first_coordinate['lat']}"
            high = round(float(string_report_obj["high"]), 2)
            back = round(float(string_report_obj["back"]), 2)
            temperature_difference = round(high - back, 2) 
            return {
                "faultType": string_report_obj['faultType'],
                "high": high,
                "panelName":string_report_obj['panelName'],
                "faultInfo":string_report_obj['faultInfo'],
                "averageTemperature": round(float(string_report_obj["mean"]), 2),
                "temperatureDifference": temperature_difference,
                "taskId":string_report_obj["taskId"],
                "zoneId":string_report_obj["zoneId"],
                "GPS":gps_string,
                "irImage":string_report_obj["irImage"],
                "rgbImage":string_report_obj["rgbImage"],
            }
    if not found_flag:
        print("No panelName map location_id")
        return {
                "faultType": '',
                "high": '',
                "panelName":'',
                "faultInfo":'',
                "averageTemperature": '',
                "temperatureDifference": '',     
                "taskId": '',
                "zoneId": '',
                "GPS":'',
                "irImage": '',
                "rgbImage": '',
            }

def get_string_data_fromdb(station_name, date, box_id, inverter_id, string_id):
    date_obj = datetime.strptime(date, '%Y-%m-%d')
    selected_string_id=string_id
    end_time = date_obj - timedelta(days=1)
    end_time = end_time.replace(hour=23, minute=59, second=59)
    start_time = end_time - timedelta(days=6)
    start_time = start_time.replace(hour=0, minute=0, second=0)
    start_date = int(time.mktime(start_time.timetuple()))
    end_date = int(time.mktime(end_time.timetuple()))

    # 连接到数据库
    conn = sqlite3.connect('./database/datang.db')
    cursor = conn.cursor()

    # 查询相同 box_id 和 inverter_id 下所有 string_id 的数据
    query_all_info = f'''
    SELECT string_id, timestamp, intensity 
    FROM {station_name}StringInfo
    WHERE timestamp BETWEEN ? AND ? 
    AND box_id = ? 
    AND inverter_id = ?
    '''
    
    all_info_data = cursor.execute(query_all_info, (start_date, end_date, box_id, inverter_id))
    all_info_rows = all_info_data.fetchall()

    # 去重时间戳
    timestamps = set()
    string_data_dict = {}
   
    for row in all_info_rows:
        string_id, timestamp, intensity = row
        timestamps.add(timestamp)  
        
        if string_id not in string_data_dict:
            string_data_dict[string_id] = []  
        string_data_dict[string_id].append(intensity)  

  
    string_data = {
        "string_timestamp_arr": sorted(timestamps),  
        "string_data_arr": string_data_dict.get(selected_string_id, []),  
        "string_assist_data": [values for key, values in string_data_dict.items() if key != selected_string_id]  
    }
    
    cursor.close()
    conn.close()
    return string_data
   
# get_station_diagnosis('datu',"2024-12-17",'original','day')
# get_string_diagnosis("datu",'2024-10-31',"001","001","001")


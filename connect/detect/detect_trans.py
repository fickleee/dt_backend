import os
import json
def _get_result_file_path(date, station_name, repo_abs_path):
    """
    构建结果文件的完整路径
    
    Args:
        date (str): 日期，格式为 "YYYY-MM-DD"
        station_name (str): 电站名称
        
    Returns:
        str: JSON文件的完整路径
    """
    results_path = os.path.join(repo_abs_path, 'data', station_name, 'results', f"{date}.json")
    return results_path

def process_anomaly_history(date, station_name, selectString, repo_abs_path):
    """
    从日期对应的 JSON 文件中读取异常日期数据
    
    Args:
        date (str): 日期，格式为 "YYYY-MM-DD"
        station_name (str, optional): 电站名称，暂时不使用
        
    Returns:
        dict: 格式为 {"BTxxx-Ixxx-PVx": [0,1,0,...]} 的字典，其中数组表示每天是否异常
    """
    file_path = _get_result_file_path(date, station_name, repo_abs_path)
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            results_data = json.load(f)
        anomaly_dates = {}
        results = results_data.get('results', {})
        for device_id, device_data in results.items():
            dates = device_data.get('anomaly_dates', [])
            box_id = device_id.split('-')[0]
            inverter_id = device_id.split('-')[1]
            string_id = str(int(device_id.split('-')[2]))  # 转为整数再转回字符串，去掉前导零
            pid = f"BT{box_id}-I{inverter_id}-PV{string_id}"
            if pid != selectString:
                continue
            anomaly_dates[pid] = dates
        return anomaly_dates
    except FileNotFoundError:
        return {}
    except Exception as e:
        print(f"Error processing anomaly dates: {str(e)}")
        return {}

def process_rdc_positions(date, station_name, selectString, repo_abs_path):
    """
    从指定日期的JSON文件中读取30天的降维坐标数据
    
    Args:
        date (str): 日期，格式为 "YYYY-MM-DD"
        station_name (str): 电站名称
        pv_name (str): 组串名称 BTxxx-Ixxx-PVx

    Returns:
        dict: 格式为 {
            "BTxxx-Ixxx-PVx": {
                "x": [x1, x2, ...],
                "y": [y1, y2, ...]
            }
        }
    """
    file_path = _get_result_file_path(date, station_name, repo_abs_path)
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            results_data = json.load(f)
        rdc_positions = {}
        results = results_data.get('results', {})
        for device_id, device_data in results.items():
            if device_data.get('anomaly_identifier') != 'normal':
                continue
            positions = device_data.get('rdc_posistion', [])
            if not isinstance(positions, list):
                continue
            
            # 转换设备ID
            box_id = device_id.split('-')[0]
            inverter_id = device_id.split('-')[1]
            string_id = str(int(device_id.split('-')[2]))
            pid = f"BT{box_id}-I{inverter_id}-PV{string_id}"
            if pid != selectString:
                continue
            
            # 初始化设备数据结构
            rdc_positions[pid] = {
                "x": [],
                "y": []
            }
            
            # 分离x和y坐标
            for pos in positions:
                if len(pos) == 2:
                    rdc_positions[pid]["x"].append(pos[0])
                    rdc_positions[pid]["y"].append(pos[1])
        
        return rdc_positions
        
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        return {}
    except Exception as e:
        print(f"Error processing RDC positions: {str(e)}")
        return {}

def process_degradation_list(date, station_name, repo_abs_path):
    """
    从日期对应的 JSON 文件中读取数据并转换为树状结构
    
    Args:
        date (str): 日期，格式为 "YYYY-MM-DD"
        station_name (str): 电站名称
        
    Returns:
        dict: 树状结构数据
    """
    file_path = _get_result_file_path(date, station_name, repo_abs_path)
    
    try:
        # 读取JSON文件
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        # 创建根节点
        tree = {
            "name": "datu",
            "level": 1,
            "key": "DTZJJK,CDTGF,Q1",
            "degradeRate": "",
            "anomalyValue": "",
            "children": []
        }
        
        # 用于临时存储不同层级的节点
        box_nodes = {}
        inverter_nodes = {}
        
        # 处理每个设备的数据
        results = data.get('results', {})
        for device_id, device_data in results.items():
            box_id, inverter_id, string_id = device_id.split('-')
            
            degrade_rate = device_data.get('degradation_score', 0)

            
            # 获取异常标识符
            anomaly_identifier = device_data.get('anomaly_identifier', 'normal')
            if anomaly_identifier == 'zero' or degrade_rate == 'N/A':
                degrade_rate = ''
            # 处理汇流箱层级 - 使用三位数格式
            box_key = f"DTZJJK,CDTGF,Q1,BT{box_id.zfill(3)}"
            if box_key not in box_nodes:
                box_node = {
                    "name": f"{box_id.zfill(3)}号箱变器",
                    "level": 2,
                    "key": box_key,
                    "degradeRate": "",
                    "anomalyValue": "",
                    "children": []
                }
                box_nodes[box_key] = box_node
                tree["children"].append(box_node)
            
            # 处理逆变器层级 - 使用三位数格式
            inverter_key = f"{box_key},I{inverter_id.zfill(3)}"
            if inverter_key not in inverter_nodes:
                inverter_node = {
                    "name": f"{inverter_id.zfill(3)}号逆变器",
                    "level": 3,
                    "key": inverter_key,
                    "degradeRate": "",
                    "anomalyValue": "",
                    "children": []
                }
                inverter_nodes[inverter_key] = inverter_node
                box_nodes[box_key]["children"].append(inverter_node)
            
            # 处理组串层级 - string_id 不需要补齐
            string_id_no_zeros = str(int(string_id))
            anomaly_value = "零电流" if anomaly_identifier == "zero" else "单口接两串" if anomaly_identifier == "double" else device_data.get('anomaly_score', 0)
            string_node = {
                "name": f"{string_id_no_zeros.zfill(3)}号组串电流",
                "level": 4,
                "key": f"{inverter_key},PVINV_DCI{string_id_no_zeros}",
                "degradeRate": degrade_rate,
                "anomalyValue": anomaly_value,
                "anomalyIdentifier": anomaly_identifier,
                "diagnosisResults": device_data.get('diagnosis_results', []),
                "historyIntensity": device_data.get('history_intensity', []),
            }
            inverter_nodes[inverter_key]["children"].append(string_node)
        
        return tree
        
    except FileNotFoundError:
        print(f"File not found: {file_path}")
        return {}
    except Exception as e:
        print(f"Error processing degradation list: {str(e)}")
        return {}

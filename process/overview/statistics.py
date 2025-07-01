import json
import os
from datetime import datetime, timedelta
import logging

# 日志配置（只需在模块顶部配置一次即可）
logger = logging.getLogger(__name__)

def get_cumulative_data(repo_abs_path, station_name, process_date):
    # process_date 格式为 'YYYY-MM-DD'，我需要前一天的日期
    prev_date = (datetime.strptime(process_date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
    prev_json_file_path = os.path.join(repo_abs_path, 'data', station_name, 'results', f'{prev_date}.json')

    cumulative_data = {
        'cumulative_fault_string': 0, # 累计故障组串数
        'cumulative_fault_inverter': 0 # 累计故障逆变器数
    }

    if not os.path.exists(prev_json_file_path):
        return cumulative_data

    try:
        with open(prev_json_file_path, 'r', encoding='utf-8') as file:
            prev_json_data = json.load(file)
        statistics = prev_json_data.get('statistics', {})
        cumulative_data['cumulative_fault_string'] += statistics.get('cumulative_fault_string', 0)
        cumulative_data['cumulative_fault_inverter'] += statistics.get('cumulative_fault_inverter', 0)
    except Exception as e:
        # 文件损坏或读取异常时, 记录日志并返回初始值
        logger.error(f"Error reading previous JSON file {prev_json_file_path}: {e}. Returning initial cumulative data.")
        return cumulative_data

    return cumulative_data


def statistics_json_file(repo_abs_path, station_name, process_date):
    # 首先获取前一天的累计数据
    cumulative_data = get_cumulative_data(repo_abs_path, station_name, process_date) # 获取场站的累积故障数(组串级和逆变器级)

    json_file_path = os.path.join(repo_abs_path, 'data', station_name, 'results', f'{process_date}.json')

    with open(json_file_path, 'r', encoding='utf-8') as file:
        json_data = json.load(file)

    total_strings = 0 # 场站当日的组串总数
    fault_string_count = 0 # 场站当日的故障组串数
    cumulative_loss = 0 # 场站的累计损失量
    fault_string_dict = { # 场站当日的故障组串分布，记录每种故障类型的数量
        "表面污迹" : 0,
        "二极管故障": 0,
        "组串开路或短路" :0
    }

    fault_inverter_set = set() # 故障逆变器集合
    loss_energy = 0 # 场站当日的预估损失量
    future_week_loss = 0 # 场站未来一周的总预估损失量
    
    data_results = json_data.get('results', {})
    # 遍历 data_results 中的每个键值对
    for string_id, value in data_results.items():
        # 1. 统计组串故障
        diagnosis_result = value.get('diagnosis_results', [])
        # 如果 diagnosis_result 不为空列表，则计入故障数量
        if diagnosis_result:
            fault_string_count += 1
            # 取形如"001-002-003"组串编号的最后一个"-"前的所有内容，作为逆变器号，加入集合
            inverter_id = string_id.rsplit('-', 1)[0]
            fault_inverter_set.add(inverter_id)
            fault_type = diagnosis_result[0].get("result", 'unknown')  # 获取故障类型
            if fault_type not in fault_string_dict:
                logger.warning(f"Unknown fault type '{fault_type}' encountered in string '{string_id}'.")
            else:
                # 增加该故障类型的计数
                fault_string_dict[fault_type] += 1
        
        # 2. 统计损失量
        ## 2.1 统计当日损失量
        history_loss = value.get('history_loss', []) 
        if history_loss:
            # 取最后一个损失量
            last_loss = history_loss[-1]/1000  # 转换为千瓦时
            loss_energy += last_loss
        ## 2.2 统计未来损失量
        future_loss = value.get('future_loss', [])
        if future_loss:
            # 取未来一周的损失量
            future_week_loss += sum(future_loss)/1000  # 转换为千瓦时
        ## 2.3 统计累计损失量
        accumulated_loss = value.get('accumulated_loss', 0)
        cumulative_loss += accumulated_loss # 已经为千瓦时,无需转换

        # 3. 统计组串总数
        total_strings += 1
    
    # 3. 统计逆变器故障（求集合中元素的数量）
    fault_inverter_count = 0
    if fault_inverter_set:
        fault_inverter_count = len(fault_inverter_set)

    # 4. 统计组串故障率
    fault_string_rate = 0
    total_strings = len(data_results)
    if total_strings > 0:
        fault_string_rate = fault_string_count / total_strings # 场站当日的组串故障率

    # 5. 统计或更新累计数据
    cumulative_data['cumulative_loss'] = cumulative_loss
    cumulative_data['cumulative_fault_string'] += fault_string_count # 更新场站的累计故障组串数
    cumulative_data['cumulative_fault_inverter'] += fault_inverter_count # 更新场站的累计故障逆变器数

    # 6. 合并成最终结果
    statistics = dict()
    ## 6.1 添加累计数据
    statistics['cumulative_loss'] = cumulative_data['cumulative_loss']/10000 # 场站的累计损失量，单位为万千瓦时
    statistics['cumulative_fault_string'] = cumulative_data['cumulative_fault_string'] # 场站的累计故障组串数
    statistics['cumulative_fault_inverter'] = cumulative_data['cumulative_fault_inverter'] # 场站的累计故障逆变器数
    ## 6.2 添加当前数据
    statistics['fault_string_count'] = fault_string_count # 场站当日的故障组串数
    statistics['fault_inverter_count'] = fault_inverter_count # 场站当日的故障逆变器数
    statistics['fault_string_rate'] = fault_string_rate # 场站当日的组串故障率
    statistics['loss_energy'] = loss_energy/10000 # 场站当日的预估损失量，单位为万千瓦时
    statistics['future_week_loss'] = future_week_loss/10000 # 场站未来一周的总预估损失量，单位为万千瓦时
    statistics['total_strings'] = total_strings # 场站当日的组串总数
    ## 6.3 添加故障组串类型统计
    statistics['fault_string_dict'] = fault_string_dict # 场站当日的故障组串分布，记录每种故障类型的数量

    return statistics
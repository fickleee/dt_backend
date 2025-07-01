import os
import json
from sqlalchemy.exc import SQLAlchemyError
import logging
from process.overview.energy import query_generation, query_plan_energy
from process.overview.impute import get_impute_info
from process.overview.statistics import statistics_json_file
from process.overview.platform import export_report
from process.overview.template import OVERVIEW_TEMPLATE
from process.overview.map import generate_map_data

# 日志配置（只需在模块顶部配置一次即可）
logger = logging.getLogger(__name__)

ANOMALY_MAPPINGS = {
    "表面污迹": "surfaceStain",
    "二极管故障": "diodeFault",
    "组串开路或短路": "circuitFault"
}

def write_statistics2json(repo_abs_path, station_name, process_date, result_dict):
    """
    将统计结果写入 JSON 文件
    """
    json_file_path = os.path.join(repo_abs_path, 'data', station_name, 'results', f'{process_date}.json')

    # 1 读取现有的json文件，增加存在性和损坏校验
    json_data = {}
    if os.path.exists(json_file_path):
        try:
            with open(json_file_path, 'r', encoding='utf-8') as file:
                json_data = json.load(file)
        except Exception as e:
            logger.warning(f"{json_file_path} 文件损坏或无法读取，将重建文件: {e}")
            json_data = {}
    else:
        logger.info(f"{json_file_path} 文件不存在，将创建新文件。")
        json_data = {}

    json_data['statistics'] = result_dict

    # 2 写入更新后的json文件
    with open(json_file_path, 'w', encoding='utf-8') as file:
        json.dump(json_data, file, ensure_ascii=False)

def post_schedule(start_timestamp, end_timestamp, repo_abs_path, database_manager, station_model, station_name, process_date, kairosdb_url, impute_model=None, token=None):
    logger.info(f"{station_name}_overview started at {process_date}")

    logger.info(f"\t{station_name}_step0 start: generate map data")
    generate_map_data(repo_abs_path, station_name, process_date)  # 生成当前日期的地图数据

    logger.info(f"\t{station_name}_step1 start: get generation data")
    # 获取日发电量、总发电量、月发电量（默认单位为 kWh）
    generated_energy, sum_energy, month_energy = query_generation(start_timestamp, end_timestamp, database_manager, station_model, station_name)

    logger.info(f"\t{station_name}_step2 start: get generation of plan")
    # 获取月计划发电量（默认单位为 kWh）
    plan_energy = query_plan_energy(process_date, kairosdb_url, station_name)

    logger.info(f"\t{station_name}_step3 start: get impute info")
    # 获取数据异常率
    data_error_rate = get_impute_info(start_timestamp, database_manager, station_name, impute_model) # 获取数据异常率

    logger.info(f"\t{station_name}_step4 start: get statistics data")
    # 获取统计数据，包含累积损失量(单位为 万千瓦时)、累积组串故障数、累积逆变器故障数、当日组串数、当日组串故障数、当日逆变器故障数、组串故障率(0-1)、当日损失量(单位为 万千瓦时)、各故障类型统计
    statistics_dict = statistics_json_file(repo_abs_path, station_name, process_date)  # 获取统计数据

    # 组装结果数据
    result_dict = dict()
    result_dict['generated_energy'] = generated_energy/10000 # 日发电量，单位为万 千瓦时
    result_dict['sum_energy'] = sum_energy/10000 # 总发电量，单位为万 千瓦时
    result_dict['month_energy'] = month_energy/10000 # 月发电量，单位为万 千瓦时
    result_dict['plan_energy'] = plan_energy/10000 # 月计划发电量，单位为万 千瓦时
    result_dict['data_error_rate'] = data_error_rate
    ## 将统计数据合并到结果字典中
    result_dict.update(statistics_dict)

    logger.info(f"\t{station_name}_step5 start: write to json file")
    # 将结果写入 JSON 文件
    write_statistics2json(repo_abs_path, station_name, process_date, result_dict)

    logger.info(f"{station_name}_overview completed at {process_date}")

    if token is not None:
        try:
            export_report(repo_abs_path, station_name, process_date, token)
        except Exception as e:
            logger.error(f"Failed to export report for {station_name} at {process_date}: {e}")
        
def overview_process(repo_abs_path, process_date, station_list):
    statistics_dict = dict()
    statistics_dict['dailyGeneration'] = 0
    statistics_dict['monthlyGeneration'] = 0
    statistics_dict['cumulativeGeneration'] = 0
    statistics_dict['cumulativeLossGeneration'] = 0
    statistics_dict['cumulativeFaultInverterDetection'] = 0
    statistics_dict['cumulativeFaultDetection'] = 0
    statistics_dict['toOMInverterFault'] = 0
    statistics_dict['toOMFault'] = 0
    statistics_dict['estimatedLoss'] = 0
    statistics_dict['stringAnomalyData'] = {
        "surfaceStain": 0,
        "diodeFault": 0,
        "circuitFault": 0
    }

    station_dict = dict()
    for station_name in station_list:
        daily_json_path = os.path.join(repo_abs_path, 'data', station_name, 'results', f'{process_date}.json')
        # 读取 JSON 文件，异常时所有数据设为0
        try:
            with open(daily_json_path, 'r', encoding='utf-8') as file:
                daily_data = json.load(file)
            station_statistics = daily_data.get('statistics', {})
        except Exception as e:
            logger.warning(f"{daily_json_path} 文件不存在或损坏，所有统计数据设为0: {e}")
            station_statistics = {}

        # 需要统计的场站级数据
        station_dict[station_name] = dict()
        station_dict[station_name]['planPowerGeneration'] = station_statistics.get('plan_energy', 0)
        station_dict[station_name]['powerGeneration'] = station_statistics.get('month_energy', 0)
        station_dict[station_name]['auxiliaryPowerRate'] = station_statistics.get('generated_energy', 0)
        station_dict[station_name]['dataAnomalyRate'] = station_statistics.get('data_error_rate', 0)
        station_dict[station_name]['lowEfficiencyAnomalyRate'] = station_statistics.get('fault_string_rate', 0)
        station_dict[station_name]['inefficientStringNumber'] = station_statistics.get('fault_string_count', 0)

        # 需要统计的全局数据
        statistics_dict['dailyGeneration'] += station_statistics.get('generated_energy', 0)
        statistics_dict['monthlyGeneration'] += station_statistics.get('month_energy', 0)
        statistics_dict['cumulativeGeneration'] += station_statistics.get('sum_energy', 0)
        statistics_dict['cumulativeLossGeneration'] += station_statistics.get('cumulative_loss', 0)
        statistics_dict['cumulativeFaultInverterDetection'] += station_statistics.get('cumulative_fault_inverter', 0)
        statistics_dict['cumulativeFaultDetection'] += station_statistics.get('cumulative_fault_string', 0)
        statistics_dict['toOMInverterFault'] += station_statistics.get('fault_inverter_count', 0)
        statistics_dict['toOMFault'] += station_statistics.get('fault_string_count', 0)
        statistics_dict['estimatedLoss'] += station_statistics.get('loss_energy', 0)
        statistics_dict['stringAnomalyData']['surfaceStain'] += station_statistics.get('fault_string_dict', {}).get('表面污迹', 0)
        statistics_dict['stringAnomalyData']['diodeFault'] += station_statistics.get('fault_string_dict', {}).get('二极管故障', 0)
        statistics_dict['stringAnomalyData']['circuitFault'] += station_statistics.get('fault_string_dict', {}).get('组串开路或短路', 0)

    # 数据统计完毕，开始更新 overview.json 文件
    overview_json_path = os.path.join(repo_abs_path, 'config', 'overview.json')
    # 读取现有的overview.json文件，异常时用模板
    try:
        with open(overview_json_path, 'r', encoding='utf-8') as file:
            overview_data = json.load(file)
    except Exception as e:
        logger.warning(f"{overview_json_path} 文件不存在或损坏，使用模板数据: {e}")
        overview_data = OVERVIEW_TEMPLATE.copy()

    # 更新数据
    overview_data['dailyGeneration'] = int(statistics_dict['dailyGeneration'])
    overview_data['monthlyGeneration'] = int(statistics_dict['monthlyGeneration'])
    overview_data['cumulativeGeneration'] = int(statistics_dict['cumulativeGeneration'])
    overview_data['cumulativeLossGeneration'] = int(statistics_dict['cumulativeLossGeneration'])
    overview_data['cumulativeFaultInverterDetection'] = statistics_dict['cumulativeFaultInverterDetection']
    overview_data['cumulativeFaultDetection'] = statistics_dict['cumulativeFaultDetection']
    overview_data['toOMInverterFault'] = statistics_dict['toOMInverterFault']
    overview_data['toOMFault'] = statistics_dict['toOMFault']
    overview_data['estimatedLoss'] = int(statistics_dict['estimatedLoss'])
    overview_data['stringAnomalyData'] = statistics_dict['stringAnomalyData']

    station_data = overview_data.get('stationData', [])
    for station in station_data:
        station_name = station.get('label')
        station['planPowerGeneration'] = station_dict.get(station_name, {}).get('planPowerGeneration', 0)
        station['powerGeneration'] = station_dict.get(station_name, {}).get('powerGeneration', 0)
        station['auxiliaryPowerRate'] = station_dict.get(station_name, {}).get('auxiliaryPowerRate', 0)
        station['dataAnomalyRate'] = station_dict.get(station_name, {}).get('dataAnomalyRate', 0)
        station['lowEfficiencyAnomalyRate'] = station_dict.get(station_name, {}).get('lowEfficiencyAnomalyRate', 0)
        station['inefficientStringNumber'] = station_dict.get(station_name, {}).get('inefficientStringNumber', 0)

    overview_data['stationData'] = station_data

    # 写入更新后的overview.json文件
    with open(overview_json_path, 'w', encoding='utf-8') as file:
        json.dump(overview_data, file, ensure_ascii=False)
        

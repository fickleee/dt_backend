import torch
import pandas as pd
import json
import numpy as np
from process.predict.mlp import MLP
import datetime
import pytz
import os
import re
import logging
from sqlalchemy import func
from sklearn.linear_model import LinearRegression
from process.predict.utils import date2timestamp, normalize, denormalize

# 日志配置（只需在模块顶部配置一次即可）
logger = logging.getLogger(__name__)

def load_model_and_params(repo_abs_path, station_name):
    """
    加载模型和全局参数
    """
    model_path = os.path.join(repo_abs_path, 'process', 'predict', "models", station_name, f'mlp_model.pth')
    params_path = os.path.join(repo_abs_path, 'process', 'predict', "models", station_name, f'global_params.json')
    with open(params_path, 'r') as f:
        global_params = json.load(f)
    model = MLP()
    device = torch.device('cpu')
    model.load_state_dict(torch.load(model_path, map_location=device))
    model.to(device)
    model.eval()
    return model, device, global_params

def calculate_loss(device_id, actual_power, inverter_predicted_power):
    inverter_pattern = r'^(\d{3}-\d{3})-\d{3}'
    # 从device_id中提取逆变器编号
    match = re.match(inverter_pattern, device_id)
    if not match:
        logger.warning(f"current device_id({device_id}) not in inverters, loss_power will be 0")
        return 0, 0  # 返回损失量和劣化率
    
    inverter_id = match.group(1)
    predicted_power = inverter_predicted_power.get(inverter_id, 0)
    loss_power = max(predicted_power - actual_power, 0)
    
    # 计算劣化率：损失量/预测量
    degradation_score = 0
    if predicted_power > 0:
        degradation_score = loss_power / predicted_power
    # 太大的劣化率不靠谱，可能是组串数据异常导致的
    if degradation_score > 0.9:
        degradation_score = 0

    return loss_power, degradation_score

def generate_inverters_loss(process_date, station_name, global_params, model, device, database_manager=None, station_model=None):
    """
    生成输入数据并预测各逆变器功率
    """
    inverter_predicted_power = dict()
    start_timestamp, end_timestamp = date2timestamp(process_date)
    station_info, _, _ = station_model

    try:
        with database_manager.get_session(station_name) as session:
            # 查询场站辐照度数据
            station_query = (
                session.query(
                    station_info.timestamp,  # 时间戳
                    station_info.irradiance  # 辐照度
                )
                .filter(station_info.timestamp >= start_timestamp)
                .filter(station_info.timestamp < end_timestamp)
                .order_by(station_info.timestamp)
                .all()
            )

            # 转换为DataFrame格式
            df = pd.DataFrame([
                {
                    'timestamp': row.timestamp,
                    'date': datetime.datetime.fromtimestamp(
                        row.timestamp, 
                        pytz.timezone('Asia/Shanghai')
                    ).strftime('%Y-%m-%d %H:%M:%S'),
                    'irradiance': row.irradiance
                }
                for row in station_query
            ])

            # 只保留 9:00-18:00 的数据
            df['date'] = pd.to_datetime(df['date'])
            df = df[(df['date'].dt.time >= datetime.time(9,0)) & (df['date'].dt.time <= datetime.time(18,0))]

            # 遍历每个逆变器，预测功率
            with torch.no_grad():
                for inverter_id, ratio in global_params['ratio'].items():
                    inverter_df = df.copy()
                    inverter_df['ratio'] = ratio
                    inverter_df['hour'] = inverter_df['date'].dt.hour / 23
                    inverter_df['irradiance'] = normalize(inverter_df['irradiance'], global_params['irradiance'])
                    X_test = inverter_df[['hour', 'ratio', 'irradiance']].values
                    X_test_tensor = torch.tensor(X_test, dtype=torch.float32).to(device)
                    predictions = model(X_test_tensor)
                    predictions_denorm = denormalize(predictions.cpu().numpy(), global_params['power'])
                    inverter_predicted_power[inverter_id] = predictions_denorm.sum()
            
            return inverter_predicted_power

    except Exception as e:
        logger.error(f"Error generating inverter loss for station '{station_name}' on date '{process_date}': {e}")
        return dict()
    
def calculate_string_loss(process_date, station_name, inverter_predicted_power, database_manager=None, station_model=None):
    start_timestamp, end_timestamp = date2timestamp(process_date)  
    _, _, string_info = station_model

    try:
        with database_manager.get_session(station_name) as session:
            # 查询
            results = (
                session.query(
                    string_info.device_id,
                    func.sum((string_info.intensity * string_info.voltage) / 6).label('total_sum') # 测试用
                    # func.sum((string_info.fixed_intensity * string_info.fixed_voltage) / 6).label('total_sum') # 部署用
                )
                .filter(string_info.timestamp >= start_timestamp)
                .filter(string_info.timestamp < end_timestamp)
                .filter(string_info.intensity != None)
                .filter(string_info.voltage != None)
                .group_by(string_info.device_id)
            ).all()

            # 将查询结果转换为字典
            result_dict = {}
            for row in results:
                loss_power, degradation_score = calculate_loss(row.device_id, row.total_sum, inverter_predicted_power)
                result_dict[row.device_id] = {
                    'loss_power': loss_power,
                    'degradation_score': degradation_score
                }
            return result_dict

    except Exception as e:
        logger.error(f"Error calculating string loss for station '{station_name}' on date '{process_date}': {e}")
        return dict()

def predict_group_next7days(history, window_size=7, predict_days=7):
    history = list(map(float, history))
    if len(history) < window_size + predict_days:
        return [0] * predict_days
    X = []
    y = []
    for i in range(len(history) - window_size - predict_days + 1):
        X.append(history[i:i+window_size])
        y.append(history[i+window_size])
    X = np.array(X)
    y = np.array(y)
    try:
        model = LinearRegression()
        model.fit(X, y)
        last_window = history[-window_size:]
        preds = []
        for _ in range(predict_days):
            pred = model.predict([last_window])[0]
            preds.append(float(pred))
            last_window = last_window[1:] + [pred]
        return preds
    except Exception:
        return [0] * predict_days


def inference_loss(history_loss):
    """
    使用历史损失预测未来损失
    - 历史损失是一个列表，包含最近30天的损失量
    - 未来损失是一个列表，包含未来7天的预测损失量
    """
    if len(history_loss) < 30:
        logger.warning("the history_loss is less than 30 days, use the average loss for prediction")
        return [np.mean(history_loss)] * 7  # 如果历史数据不足30天，则返回全平均值

    future_loss = predict_group_next7days(history_loss[-30:], window_size=7, predict_days=7)
    return future_loss

def history2future_loss(history_loss):
    """
    将历史损失转换为未来损失
    - 历史损失是一个列表，包含最近30天的损失量
    - 未来损失是一个列表，包含未来7天的预测损失量
    """
    future_loss = dict()
    for device_id, loss_list in history_loss.items():
        future_loss[device_id] = inference_loss(loss_list)
    return future_loss

def write_history_loss(process_date, station_name, repo_abs_path, string_loss): 
    """
    该函数用于将每个组串的当日损失量(loss_power)和劣化率(degradation_score)写入对应日期的日志文件，实现历史损失的累计和更新。
    - 如果存在昨天的日志，则在昨天的历史损失基础上追加今天的损失，并只保留最近30天。
    - 如果不存在昨天的日志，则以今天的损失为起点。
    - 如果今天的日志文件已存在，则只更新每个组串的损失量。
    - 如果今天的日志文件不存在，则创建新文件。
    """
    yesterday_date = (datetime.datetime.strptime(process_date, "%Y-%m-%d") - datetime.timedelta(days=1)).strftime("%Y-%m-%d")

    log_path = os.path.join(repo_abs_path, 'data', station_name, "results", f"{process_date}.json")
    yesterday_path = os.path.join(repo_abs_path, 'data', station_name, "results", f"{yesterday_date}.json")

    history_dict = dict() # 包含每个组串的历史损失量(列表, 最多30个元素,最后一个元素应为当日损失量)

    if os.path.exists(yesterday_path):
        with open(yesterday_path, 'r') as f:
            try:
                yesterday_data = json.load(f)
                # 合并昨天的历史损失，并追加今天的损失
                for device_id, loss_dict in yesterday_data.get("results", {}).items():
                    history_loss_list = loss_dict.get("history_loss", [])
                    # 追加今天的损失
                    current_loss = string_loss.get(device_id, {}).get('loss_power', 0)
                    history_loss_list.append(current_loss)
                    # 保留最近30天
                    if len(history_loss_list) > 30:
                        history_loss_list = history_loss_list[-30:]
                    history_dict[device_id] = history_loss_list
            except json.JSONDecodeError as e:
                logger.warning(f"昨日历史损失文件损坏，忽略昨日数据: {e}")
                history_dict = {device_id: [string_loss.get(device_id, {}).get('loss_power', 0)] for device_id in string_loss.keys()}
    else:
        # 没有昨天的日志，则以今天的损失为起点
        history_dict = {device_id: [string_loss.get(device_id, {}).get('loss_power', 0)] for device_id in string_loss.keys()}

    # 预测未来的损失
    future_dict = history2future_loss(history_dict) # 包含每个组串的未来损失量(列表, 7个元素)

    # 获取前一天的劣化率数据和累计损失数据
    yesterday_degradation_dict = {} # 包含每个组串的昨天的劣化率
    yesterday_accumulated_loss_dict = {} # 包含每个组串的昨天的累计损失量
    if os.path.exists(yesterday_path):
        with open(yesterday_path, 'r') as f:
            try:
                yesterday_data = json.load(f)
                for device_id, loss_dict in yesterday_data.get("results", {}).items():
                    yesterday_degradation_dict[device_id] = loss_dict.get("degradation_score", 0)
                    yesterday_accumulated_loss_dict[device_id] = loss_dict.get("accumulated_loss", 0)
            except json.JSONDecodeError as e:
                logger.warning(f"昨日历史损失文件损坏，无法获取昨日数据: {e}")

    # 判断今天的日志文件是否存在
    if not os.path.exists(log_path):
        # 不存在则创建新日志
        log_dict = {
            "date": process_date,
            "results": {}
        }
        for device_id, loss_list in history_dict.items():
            current_degradation = string_loss.get(device_id, {}).get('degradation_score', 0)  # 获取当前组串的劣化率
            
            # 检查前一天的劣化率，确保劣化率单调不减
            previous_degradation = yesterday_degradation_dict.get(device_id, 0)
            if current_degradation < previous_degradation:
                current_degradation = previous_degradation
            
            # 计算累计损失量
            today_loss = loss_list[-1] if loss_list else 0  # 今天的损失量（Wh）
            previous_accumulated_loss = yesterday_accumulated_loss_dict.get(device_id, 0)  # 前一天的累计损失量（kWh）
            
            # 将今天的损失量从 Wh 转换为 kWh，然后加到累计损失量中
            today_loss_kwh = today_loss / 1000  # 转换为 kWh
            accumulated_loss = previous_accumulated_loss + today_loss_kwh  # 累计损失量以 kWh 为单位存储
            
            log_dict["results"][device_id] = {
                "history_loss": loss_list, 
                "future_loss": future_dict.get(device_id, []),
                "degradation_score": current_degradation,
                "accumulated_loss": accumulated_loss
            }
        with open(log_path, 'w', encoding='utf-8') as f:
            json.dump(log_dict, f, ensure_ascii=False)
    else:
        # 已存在则只更新历史损失量、未来损失量、劣化率和累计损失量
        with open(log_path, 'r', encoding='utf-8') as f:
            log_dict = json.load(f)
        results = log_dict.get("results", {})
        for device_id, loss_list in history_dict.items():
            current_degradation = string_loss.get(device_id, {}).get('degradation_score', 0)
            
            # 检查前一天的劣化率，确保劣化率单调不减
            previous_degradation = yesterday_degradation_dict.get(device_id, 0)
            if current_degradation < previous_degradation:
                current_degradation = previous_degradation
            
            # 计算累计损失量
            today_loss = loss_list[-1] if loss_list else 0  # 今天的损失量（Wh）
            previous_accumulated_loss = yesterday_accumulated_loss_dict.get(device_id, 0)  # 前一天的累计损失量（kWh）
            
            # 将今天的损失量从 Wh 转换为 kWh，然后加到累计损失量中
            today_loss_kwh = today_loss / 1000  # 转换为 kWh
            accumulated_loss = previous_accumulated_loss + today_loss_kwh  # 累计损失量以 kWh 为单位存储
            
            if device_id in results:
                results[device_id]["history_loss"] = loss_list
                results[device_id]["future_loss"] = future_dict.get(device_id, [])
                results[device_id]["degradation_score"] = current_degradation
                results[device_id]["accumulated_loss"] = accumulated_loss
            else:
                results[device_id] = {
                    "history_loss": loss_list, 
                    "future_loss": future_dict.get(device_id, []),
                    "degradation_score": current_degradation,
                    "accumulated_loss": accumulated_loss
                }
        log_dict["results"] = results
        with open(log_path, 'w', encoding='utf-8') as f:
            json.dump(log_dict, f, ensure_ascii=False)

def predict_schedule(process_date, repo_abs_path, database_manager, station_models, station_name):
    logger.info(f"{station_name}_predict started at {process_date}")
    logger.info(f"\t{station_name}_step1 start: Load model and global parameters")
    model, device, global_params = load_model_and_params(repo_abs_path, station_name)
    logger.info(f"\t{station_name}_step2 start: Generate optimal inverter string power prediction")
    inverter_predicted_power = generate_inverters_loss(process_date, station_name, global_params, model, device, database_manager, station_models[station_name])
    logger.info(f"\t{station_name}_step3 start: Calculate string-level loss")
    string_loss = calculate_string_loss(process_date, station_name, inverter_predicted_power, database_manager, station_models[station_name])
    logger.info(f"\t{station_name}_step4 : Write string-level loss to log file")
    # Write string-level loss for the corresponding date and station to the log file
    write_history_loss(process_date, station_name, repo_abs_path, string_loss)

    logger.info(f"{station_name}_predict completed at {process_date}")
from flask import Flask, jsonify, request, send_file
from user.index import validate_username_exists, user_register, user_login, get_all_user, get_user_by_name, change_user_status, delete_user, edit_user, reset_password
from connect.detect.detect_trans import process_degradation_list
from connect.detect.get_history_data import get_power_loss_data

from connect.overview.index import get_overview_data,get_overview_station_map_latest,get_overview_station_info,get_json_file_state, get_overview_station_map
from connect.merge.index import get_merge_results, get_merge_map, get_merge_image, save_merge_data
# from process.merge.index import mc_pdf2jpg, get_mc_image_path, get_merged_image_path, get_merged_label_path, get_mc_geo_data_path, process_mc_image, seg_predict_merge,split_plot
# from process.merge.fusion import data_fusion
from process.impute.index import get_station_info_orm, get_station_chart_orm, get_station_origin_data_orm_optimized
from connect.impute.index import save_imputed_result_orm
from process.impute.model import impute, repair
from process.plan.index import get_plan_data, export_report, export_maintain_report_only, export_runtime_report_only, STATION_NAME_MAPPING, CENTER_TO_STATION, CENTER_NAME_MAPPING
from process.index import run_process_schedule # run_process_schedule 为定时函数,run_process_manual 为手动执行函数
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import pytz
import os
import json
from dotenv import load_dotenv
from schema.models import create_station_models, create_impute_model, create_user_model,  create_power_models
from schema.session import DatabaseManager
import zipfile
import io
import logging
import base64

app = Flask(__name__)

# 获取当前环境变量，默认为development（通过在命令行中设置）
env_name = os.getenv("APP_ENV", "development").strip()

# 设置全局变量，项目根目录的绝对路径
if env_name == "production" or env_name == "local": # 生产模式
    global_repo_abs_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'app')
else: # 开发模式
    global_repo_abs_path = os.path.dirname(os.path.abspath(__file__))

# 日志配置
logs_dir = os.path.join(global_repo_abs_path, "logs")
os.makedirs(logs_dir, exist_ok=True)
shanghai_tz = pytz.timezone("Asia/Shanghai")
log_time_str = datetime.now(shanghai_tz).strftime("%Y%m%d_%H%M%S")
log_file_path = os.path.join(logs_dir, f"run_{log_time_str}.log")

# 获取 root logger 并添加 handler
root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
file_handler = logging.FileHandler(log_file_path, encoding="utf-8")
file_handler.setFormatter(formatter)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
root_logger.handlers = [file_handler, stream_handler]

logger = root_logger  # 直接用 root logger

# 让 Flask 的 logger 也用 root logger 的 handler
app.logger.handlers = root_logger.handlers
app.logger.setLevel(logging.INFO)

# 加载环境变量配置文件
load_dotenv(os.path.join(global_repo_abs_path, 'setting', f".env.{env_name}")) 

# 设置全局变量，包括KairosDB的URL、时间窗口、数据库管理器实例和场站列表
global_kairosdb_url = os.getenv('KAIROSDB_URL', 'http://localhost:8080/api/v1/datapoints/query').strip() # 获取KairosDB的URL
global_time_window = int(os.getenv('TIME_WINDOW', '30').strip()) # 获取时间窗口，默认为30天
global_database_manager = DatabaseManager(global_repo_abs_path) # 创建数据库管理器实例
global_station_list = os.getenv('STATION_LIST', 'datu').strip().split(',') # 获取场站列表
global_schedule_time = os.getenv('SCHEDULE_TIME', '1,0').strip().split(',') # 获取定时任务执行时间，默认为01:00
global_api_username = os.getenv('API_USERNAME', 'dtzhejiang').strip()
global_api_password = os.getenv('API_PASSWORD', 'zkYs!23').strip()

# 动态创建表
global_station_models = {station_name: create_station_models(station_name) for station_name in global_station_list} # 各场站的数据表模型
global_impute_models = {station_name: create_impute_model(station_name) for station_name in global_station_list} # 各场站的impute对应的表模型
global_user_model = create_user_model() # 用户表模型
# 创建功率损失和预测表模型
global_power_models = {station_name: create_power_models(station_name) for station_name in global_station_list} # 各场站的功率损失和预测表模型

logger.info("当前运行模式：{} 数据库类型：{} 项目根目录：{} 场站列表：{}".format(env_name, os.getenv('DB_TYPE', 'sqlite').strip().lower(), global_repo_abs_path, global_station_list))

# 定义定时任务
def scheduled_task(kairosdb_url, repo_abs_path):
    try:
        logger.info(f"\t定时器函数于 {datetime.now(pytz.timezone('Asia/Shanghai'))} 开始执行，正在创建前一天的数据...")
        run_process_schedule(kairosdb_url, repo_abs_path, global_time_window, global_database_manager, global_station_models, global_impute_models, global_station_list, global_api_username, global_api_password)
        logger.info(f"\t定时器函数于 {datetime.now(pytz.timezone('Asia/Shanghai'))} 执行完成！")
    except Exception as e:
        logger.error(f"Error in scheduled_task: {e}")

# 创建调度器
scheduler = BackgroundScheduler()
scheduler.add_job(func=scheduled_task, trigger='cron', hour=int(global_schedule_time[0]), minute=int(global_schedule_time[1]), timezone=pytz.timezone('Asia/Shanghai'), args=[global_kairosdb_url, global_repo_abs_path])
scheduler.start()

# 定义不需要验证token的路径
EXCLUDE_PATHS = [
    '/api/user/login',
    '/api/user/register',
    '/api/user/validate_name',
]

# 创建一个全局的装饰器，应用到所有路由
@app.before_request
def before_request():
    # 如果是不需要验证的路径，直接返回
    if request.path in EXCLUDE_PATHS:
        return
    
    # OPTIONS请求不需要验证
    if request.method == 'OPTIONS':
        return
    
    # 验证token
    auth_header = request.headers.get('Authorization')
    if not auth_header:
        return jsonify({'message': '缺少token'}), 401
    
    try:
        token = auth_header.split(" ")[1]
        from user.jwt_handler import verify_token
        result = verify_token(token)
        if not result['valid']:
            return jsonify({'message': result['message']}), 401
        request.user = result['data']
    except Exception as e:
        return jsonify({'message': '无效的token'}), 401


###注意！需前端传入场站名字、时间（精确到日期）
#============impute api start==============
@app.route('/api/station/detail')
def station_detail():
    station_name = request.args.get('station_name')
    start_time = request.args.get('start_time')
    end_time = request.args.get('end_time')
    variable = request.args.get('variable')
    # 获取对应场站的 impute 模型
    impute_model = global_impute_models.get(station_name)
    # 调用 ORM 函数，传递数据库管理器和模型
    data = get_station_info_orm(station_name, variable, start_time, end_time, global_repo_abs_path, global_database_manager, impute_model)
    return data


@app.route('/api/station/data')
def station_data():
    station_name = request.args.get('station_name')
    start_time = request.args.get('start_time')
    end_time = request.args.get('end_time')
    variable = request.args.get('variable')
    device_id = request.args.get('device_id')
    # 获取对应场站的表模型
    station_model = global_station_models.get(station_name)
    impute_model = global_impute_models.get(station_name)
    # 调用 ORM 函数，传递数据库管理器和模型
    res = get_station_origin_data_orm_optimized(station_name, device_id, start_time, end_time, variable, global_repo_abs_path, global_database_manager, station_model, impute_model)
    return res


@app.route('/api/station/chart')
def station_chart():
    station_name = request.args.get('station_name')
    start_time = request.args.get('start_time')
    variable = request.args.get('variable')
    device_id = request.args.get('device_id')
    # 获取对应场站的表模型
    station_model = global_station_models.get(station_name)
    # 调用 ORM 函数，传递数据库管理器和模型
    res = get_station_chart_orm(station_name, device_id, start_time, variable, global_repo_abs_path, global_database_manager, station_model)
    return res


@app.route('/api/station/impute')
def station_model_impute():
    station_name = request.args.get('station_name')
    start_time = request.args.get('start_time')
    variable = request.args.get('variable')
    device_id = request.args.get('device_id')

    # 获取对应场站的表模型
    station_model = global_station_models.get(station_name)
    # 调用 impute 函数，传递数据库管理器和模型
    res = impute(station_name, device_id, start_time, variable, global_repo_abs_path, global_database_manager, station_model)
    return res


@app.route('/api/station/save', methods=['POST'])
def station_save_result():
    data = request.get_json()
    station_name = data.get('stationName')
    device_id = data.get('deviceId')
    variable = data.get('variableType')
    start_time = data.get('date')
    impute_data = data.get('imputeData')
    station_model = global_station_models.get(station_name)
    res = save_imputed_result_orm(station_name, device_id, start_time, variable, impute_data, global_repo_abs_path, global_database_manager, station_model)
    return res
# #============impute api end================



# #============repair api start==============
@app.route('/api/station/repair')
def station_model_repair():
    station_name = request.args.get('station_name')
    start_time = request.args.get('start_time')
    variable = request.args.get('variable')
    device_id = request.args.get('device_id')

    # 获取对应场站的表模型
    station_model = global_station_models.get(station_name)
    # 调用 impute 函数，传递数据库管理器和模型
    res = repair(station_name, device_id, start_time, variable, global_repo_abs_path, global_database_manager, station_model)
    return res
# #============repair api end================



#============merge api start==============
@app.route('/api/station/merge/table', methods=['GET'])  # merge界面右侧匹配表格数据的读取
def api_get_merge_results():
    station_name = request.args.get('area_name')
    return get_merge_results(station_name, global_repo_abs_path)


@app.route('/api/station/merge/geo', methods=['GET'])  # merge界面左下侧geojson地图数据的读取
def api_get_merge_map():
    station_name = request.args.get('area_name')
    return get_merge_map(station_name, global_repo_abs_path)

@app.route('/api/station/merge/image')  # merge界面左上侧图片数据的读取
def api_get_merge_image():
    station_name = request.args.get('area_name')
    file_name = request.args.get('image_name')
    return get_merge_image(station_name, file_name, global_repo_abs_path)

@app.route('/api/station/merge/saved', methods=['POST'])  # 保存 merge界面中交互式修改最优匹配和OCR的结果到原文件
def api_save_merge_data():
    data = request.json
    match_data = data.get('match_data')
    ocr_data = data.get('ocr_data')
    station_name = data.get('area_name')
    return save_merge_data(station_name,match_data,ocr_data, global_repo_abs_path)

# #===merge-create api start===
# @app.route('/api/merge-create-upload-files', methods=['POST'])
# def merge_create_upload_files():
#     if 'stationName' not in request.form:
#         return jsonify({'message': '场站名称不能为空', 'imagesList': []}), 400

#     station_name = request.form['stationName']
#     upload_dir = os.path.join(global_repo_abs_path, 'merge', station_name)
#     print(f"Upload directory: {upload_dir}")

#     try:
#         os.makedirs(upload_dir, exist_ok=True)

#         # Handle blueprint files
#         if 'bpFiles[]' in request.files:  # Changed from 'bpFiles' to match frontend
#             pdfs_dir = os.path.join(upload_dir, 'pdfs')
#             os.makedirs(pdfs_dir, exist_ok=True)
#             for file in request.files.getlist('bpFiles[]'):  # Changed to match frontend
#                 if file and file.filename:
#                     filename = secure_filename(file.filename)
#                     file.save(os.path.join(pdfs_dir, filename))

#         # Handle geo files
#         if 'geoFiles[]' in request.files:  # Changed from 'geoFiles' to match frontend
#             config_dir = os.path.join(upload_dir, 'config')
#             os.makedirs(config_dir, exist_ok=True)
#             for file in request.files.getlist('geoFiles[]'):  # Changed to match frontend
#                 if file and file.filename:
#                     filename = secure_filename(file.filename)
#                     file.save(os.path.join(config_dir, filename))

#     except Exception as e:
#         return jsonify({'message': f'上传失败: {str(e)}', 'imagesList': []}), 500

#     images_list = mc_pdf2jpg(global_repo_abs_path, station_name)
#     return jsonify({'message': '上传成功', 'imagesList': images_list}), 200

# @app.route('/api/merge-create-show-image', methods=['GET'])
# def get_merge_create_image():
#     station_name = request.args.get('stationName')
#     image_name = request.args.get('imageName')

#     image_path = get_mc_image_path(global_repo_abs_path, station_name, image_name)
#     return send_file(image_path)

# @app.route('/api/merge-create-geojson', methods=['GET'])
# def get_merge_create_geojson():
#     station_name = request.args.get('stationName')

#     geo_path, geo_label_path = get_mc_geo_data_path(global_repo_abs_path, station_name)

#     try:
#         with open(geo_path, 'r', encoding='utf-8') as geo_file:
#             panel_geo_data = json.load(geo_file)

#         with open(geo_label_path, 'r', encoding='utf-8') as label_file:
#             panel_label_data = json.load(label_file)

#         merged_data = {
#             'panel_geo': panel_geo_data,
#             'panel_geo_label': panel_label_data
#         }

#         return jsonify(merged_data)
#     except FileNotFoundError:
#         return jsonify({'error': '文件不存在'}), 404
#     except Exception as e:
#         return jsonify({'error': str(e)}), 500

# @app.route('/api/merge-create-merged-image', methods=['GET'])
# def get_merge_create_merged_image():
#     station_name = request.args.get('stationName')
#     image_name = request.args.get('imageName')

#     image_path = get_merged_image_path(global_repo_abs_path, station_name, image_name)
#     return send_file(image_path)

# @app.route('/api/merge-create-merged-label', methods=['GET'])
# def get_merge_create_merged_label():
#     station_name = request.args.get('stationName')
#     label_name = request.args.get('labelName')

#     label_path = get_merged_label_path(global_repo_abs_path, station_name, label_name)
#     return send_file(label_path)

# @app.route('/api/merge-create-save-merged-label', methods=['POST'])
# def get_merge_create_save_merged_label():
#     try:
#         data = request.get_json()
#         station_name = data.get('stationName')
#         label_name = data.get('labelName')
#         save_data = data.get('saveData', [])

#         label_path = get_merged_label_path(global_repo_abs_path, station_name, label_name)

#         # 读取原始文件
#         with open(label_path, 'r', encoding='utf-8') as f:
#             original_lines = f.readlines()

#         # 检查数据行数是否一致
#         if len(original_lines) != len(save_data):
#             return jsonify({
#                 'error': f'数据行数不匹配: 原始文件 {len(original_lines)} 行, 新数据 {len(save_data)} 行'
#             }), 400

#         # 只修改每行的最后一列（OCR文本）
#         new_lines = []
#         for i, (line, new_data) in enumerate(zip(original_lines, save_data)):
#             # 分割原始行，保留除最后一列外的所有数据
#             parts = line.strip().split()
#             # 使用新的OCR文本替换最后一列
#             parts[-1] = new_data['ocr']
#             # 重新组合行
#             new_line = ' '.join(parts) + '\n'
#             new_lines.append(new_line)

#         # 写入修改后的文件
#         with open(label_path, 'w', encoding='utf-8') as f:
#             f.writelines(new_lines)

#         return jsonify({
#             'message': '保存成功',
#             'updated_rows': len(new_lines)
#         }), 200

#     except Exception as e:
#         print(f"保存标签时出错: {str(e)}")  # 服务器端日志
#         return jsonify({'error': str(e)}), 500

# @app.route('/api/merge-create-update-merged-label', methods=['POST'])
# def get_merge_create_update_merged_label():
#     try:
#         data = request.get_json()
#         station_name = data.get('stationName')
#         label_name = data.get('labelName')
#         save_data = data.get('saveData', [])

#         label_path = get_merged_label_path(global_repo_abs_path, station_name, label_name)

#         # 读取原始文件
#         with open(label_path, 'r', encoding='utf-8') as f:
#             original_lines = f.readlines()

#         # 检查数据行数是否一致
#         if len(original_lines) != len(save_data):
#             return jsonify({
#                 'error': f'数据行数不匹配: 原始文件 {len(original_lines)} 行, 新数据 {len(save_data)} 行'
#             }), 400

#         # 只修改每行的最后一列（OCR文本）
#         new_lines = []
#         for i, (line, new_data) in enumerate(zip(original_lines, save_data)):
#             if new_data['ocr'] == '':
#                 continue

#             # 分割原始行，保留除最后一列外的所有数据
#             parts = line.strip().split()
#             # 使用新的OCR文本替换最后一列
#             parts[-1] = new_data['ocr']
#             # 重新组合行
#             new_line = ' '.join(parts) + '\n'
#             new_lines.append(new_line)

#         # 写入修改后的文件
#         with open(label_path, 'w', encoding='utf-8') as f:
#             f.writelines(new_lines)

#         return jsonify({
#             'message': '保存成功',
#             'updated_rows': len(new_lines)
#         }), 200

#     except Exception as e:
#         print(f"保存标签时出错: {str(e)}")  # 服务器端日志
#         return jsonify({'error': str(e)}), 500

# @app.route('/api/merge-create-show-image-save', methods=['POST'])
# def save_merge_create_image_save():
#     data = request.get_json()
#     station_name = data.get('stationName')
#     image_name = data.get('imageName')
#     rotation = data.get('rotation')
#     rectangles = data.get('rectangles', [])  # 直接获取数组

#     print(f"station_name: {station_name}, image_name: {image_name}, rotation: {rotation}, rectangles: {rectangles}")

#     new_image_path = process_mc_image(global_repo_abs_path, station_name, image_name, rotation, rectangles)
#     return send_file(new_image_path)

# @app.route('/api/merge-create-split', methods=['POST'])
# def get_merge_create_split():
#     data = request.get_json()
#     station_name = data.get('stationName')
#     geo_split_data = data.get('geoSplitData', [])
#     bp_split_data = data.get('bpSplitData', [])
#     split_plot(global_repo_abs_path, station_name, bp_split_data, geo_split_data)
#     data_fusion(global_repo_abs_path, station_name)
#     return jsonify({"status": "success"})

# @app.route('/api/merge-create-image-detect', methods=['GET'])
# def get_merge_create_image_detect():
#     station_name = request.args.get('stationName')
#     seg_predict_merge(global_repo_abs_path, station_name)
#     return jsonify({"status": "success"})

# #===merge-create api end===
#============merge api end================



#============detect api start==============
@app.route('/api/detect/get-file-name-list', methods=['GET'])
def get_file_name_list():
    date = request.args.get('date')
    station_name = request.args.get('station_name')
    data = process_degradation_list(date, station_name, global_repo_abs_path)
    return jsonify(data), 200


@app.route('/api/detect/get-power-loss', methods=['GET'])
def get_power_loss():
    """
    获取组串功率损失数据接口
    
    URL参数:
    - station_name: 场站名称
    - selectString: 组串标识，如 'BT001-I001-PV1'
    - date: 日期，格式为 'YYYY-MM-DD'
    
    返回:
    - history_loss: 历史30天的功率损失数据数组
    - future_loss: 预测7天的功率损失数据数组
    - history_dates: 历史日期数组（MM-DD格式）
    - future_dates: 预测日期数组（MM-DD格式）
    """
    station_name = request.args.get('station_name')
    select_string = request.args.get('selectString')
    date = request.args.get('date')

    # 获取对应场站的表模型
    station_model = global_station_models.get(station_name)
    # 获取对应场站的功率损失和预测表模型
    power_models = global_power_models.get(station_name)
    
    result = get_power_loss_data(station_name, select_string, date, global_repo_abs_path, global_database_manager, station_model, power_models)
    return jsonify(result), 200

@app.route('/api/detect/get-string-diagnosis', methods=['GET'])
def get_string_diagnosis():
    """
    获取组串诊断结果接口
    
    URL参数:
    - station_name: 场站名称
    - selectString: 组串标识，如 'BT001-I001-PV1'
    - date: 日期，格式为 'YYYY-MM-DD'
    
    返回:
    - diagnosis_results: 诊断结果数组，包含故障类型和比率
    """
    station_name = request.args.get('station_name')
    select_string = request.args.get('selectString')
    date = request.args.get('date')
    
    try:
        # 解析组串ID
        parts = select_string.split('-')
        if len(parts) != 3:
            raise ValueError(f"Invalid select_string format: {select_string}")
        
        box_id = parts[0][2:]  # 去掉 'BT' 前缀
        inverter_id = parts[1][1:]  # 去掉 'I' 前缀
        string_id = parts[2][2:]  # 去掉 'PV' 前缀
        
        # 构造查询键
        string_key = f"{box_id.zfill(3)}-{inverter_id.zfill(3)}-{string_id.zfill(3)}"
        
        # 尝试从results JSON文件获取数据
        results_file_path = os.path.join(global_repo_abs_path, 'data', station_name, 'results', f"{date}.json")
        if os.path.exists(results_file_path):
            with open(results_file_path, 'r', encoding='utf-8') as f:
                results_data = json.load(f)
            
            # 尝试获取诊断结果
            results = results_data.get('results', {})
            if string_key in results and 'diagnosis_results' in results[string_key]:
                diagnosis_results = results[string_key]['diagnosis_results']
                # 转换为百分比格式
                for item in diagnosis_results:
                    item['rate'] = round(item['rate'] * 100)
                return jsonify({'diagnosis_results': diagnosis_results}), 200
        
        # 如果没有找到数据，返回默认诊断结果
        default_results = [
            {"result": "表面污迹", "rate": 40},
            {"result": "二极管故障", "rate": 30},
            {"result": "组串开路", "rate": 20},
            {"result": "热斑", "rate": 10}
        ]
        return jsonify({'diagnosis_results': default_results}), 200
        
    except Exception as e:
        print(f"Error getting string diagnosis: {str(e)}")
        # 返回默认诊断结果
        default_results = [
            {"result": "表面污迹", "rate": 40},
            {"result": "二极管故障", "rate": 30},
            {"result": "组串开路", "rate": 20},
            {"result": "热斑", "rate": 10}
        ]
        return jsonify({'diagnosis_results': default_results}), 200

@app.route('/api/detect/get-string-uav-images', methods=['GET'])
def get_uav_imgs():
    station_name = request.args.get('station_name')
    select_string = request.args.get('selectString')
    # date = request.args.get('date')  # 不用

    # 解析 select_string
    try:
        parts = select_string.split('-')
        if len(parts) != 3:
            return jsonify({"error": "selectString格式错误"}), 400
        box_id = str(int(parts[0][2:]))
        inverter_id = str(int(parts[1][1:]))
        string_id = str(int(parts[2][2:]))
        img_id = f"{box_id},{inverter_id},{string_id}"
    except Exception:
        return jsonify({"error": "selectString解析失败"}), 400

    # 拼接图片路径
    img_dir = os.path.join(global_repo_abs_path, "data", station_name, "images")
    ir_path = os.path.join(img_dir, f"{img_id}_ir.jpg")
    rgb_path = os.path.join(img_dir, f"{img_id}_rgb.jpg")

    def img_to_base64(path):
        if os.path.exists(path):
            with open(path, "rb") as f:
                return "data:image/jpeg;base64," + base64.b64encode(f.read()).decode()
        return ""
    
    ir_b64 = img_to_base64(ir_path)
    rgb_b64 = img_to_base64(rgb_path)

    return jsonify({
        "rgb_image": rgb_b64,
        "ir_image": ir_b64
    })

#============detect api end================



#============diagnose api start==============


#============diagnose api end================



#============predict api start==============

#============predict api end================



#============plan api start==============
@app.route('/api/plan/data', methods=['GET'])
def get_plan_data_api():
    station_name = request.args.get('station_name')
    process_date = request.args.get('process_date')
    station_model = global_station_models.get(station_name)
    result = get_plan_data(station_name, process_date, global_repo_abs_path, global_database_manager, station_model)
    return result


@app.route('/api/plan/export/maintain', methods=['GET'])
def export_maintain_report_api():
    """导出运维层面报告接口"""
    station_name = request.args.get('station_name')
    process_date = request.args.get('process_date')
    station_model = global_station_models.get(station_name)

    # 生成运维层面excel文件
    maintain_path = export_maintain_report_only(station_name, process_date, global_repo_abs_path, global_database_manager, station_model)

    # 手动构造中文文件名
    station_cn = STATION_NAME_MAPPING.get(station_name, station_name)
    download_filename = f"{station_cn}_{process_date}_运维层面.xlsx"

    # 返回excel文件
    return send_file(
        maintain_path,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=download_filename
    )


@app.route('/api/plan/export/runtime', methods=['GET'])
def export_runtime_report_api():
    """导出运行层面报告接口"""
    station_name = request.args.get('station_name')
    process_date = request.args.get('process_date')
    station_model = global_station_models.get(station_name)

    # 生成运行层面excel文件
    runtime_path = export_runtime_report_only(station_name, process_date, global_repo_abs_path, global_database_manager, station_model)

    # 手动构造中文文件名
    # 获取center英文名
    target_center = None
    for center, stations in CENTER_TO_STATION.items():
        if station_name in stations:
            target_center = center
            break
    # 获取center中文名
    center_cn = CENTER_NAME_MAPPING.get(target_center, target_center or station_name)
    download_filename = f"{center_cn}_{process_date}_运行层面.xlsx"

    # 返回excel文件
    return send_file(
        runtime_path,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=download_filename
    )


@app.route('/api/plan/export', methods=['GET'])
def export_plan_report_api():
    """导出完整报告接口（包含运维和运行两个层面）"""
    station_name = request.args.get('station_name')
    process_date = request.args.get('process_date')
    station_model = global_station_models.get(station_name)

    # 生成两个excel文件，直接接收两个路径
    maintain_path, runtime_path = export_report(station_name, process_date, global_repo_abs_path, global_database_manager, station_model)

    # 手动构造中文文件名
    station_cn = STATION_NAME_MAPPING.get(station_name, station_name)
    maintain_filename = f"{station_cn}_{process_date}_运维层面.xlsx"
    runtime_filename = f"{station_cn}_{process_date}_运行层面.xlsx"
    zip_filename = f"{station_cn}_{process_date}.zip"

    # 打包成zip
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w') as zip_file:
        zip_file.write(maintain_path, maintain_filename)
        zip_file.write(runtime_path, runtime_filename)
    zip_buffer.seek(0)

    # 返回zip文件
    return send_file(
        zip_buffer,
        mimetype='application/zip',
        as_attachment=True,
        download_name=zip_filename
    )
#============plan api end================



#============overview api start==============
@app.route('/api/station/file', methods=['GET'])  # 场站概览界面，判断当前电厂当前日期的日志文件是否存在
def api_get_json_file_state():
    station_name = request.args.get('station_name')
    process_date = request.args.get('process_date')
    # process_date = '2024-10-31'
    return get_json_file_state(station_name, process_date, global_repo_abs_path)

@app.route('/api/overview/station-map', methods=['GET'])  # 场站概览界面左侧geojson地图数据的读取
def api_get_overview_station_map():
    station_name = request.args.get('station_name')
    process_date = request.args.get('process_date')
    # process_date = '2024-10-31'
    # return get_overview_station_map(station_name, process_date, global_repo_abs_path, global_time_window)
    return get_overview_station_map_latest(station_name, process_date, global_repo_abs_path)

@app.route('/api/overview/station-info', methods=['GET'])  # 场站概览界面左侧geojson地图数据的读取
def api_get_overview_station_info():
    station_name = request.args.get('station_name')
    process_date = request.args.get('process_date')
    # process_date = '2024-10-31'
    return get_overview_station_info(station_name, process_date, global_repo_abs_path)

@app.route('/api/overview/overview-data', methods=['GET'])
def api_get_overview_data():
    return get_overview_data(global_repo_abs_path)

#============overview api end================



#============user api start==============
@app.route('/api/user/validate_name', methods=['POST'])
def validate_username():
    data = request.get_json()
    username = data['username']
    result = validate_username_exists(username, global_repo_abs_path)
    return jsonify({'exist':result})

@app.route('/api/user/register', methods=['POST'])
def register():
    data = request.get_json()
    username = data['username']
    email = data['email']
    phone = data['phone']
    password = data['password']

    ret = user_register(username, password, email, phone, global_repo_abs_path)
    # ret = user_register_orm(username, password, email, phone, global_database_manager, global_user_model)

    if ret:
        status = 'success'
        msg = '用户注册成功，等待审核！'
    else:
        status = 'fail'
        msg = '用户注册失败！'

    return jsonify({
        'status': status,
        'message': msg,
        'username': username
    }), 200

@app.route('/api/user/login', methods=['POST'])
def login():
    data = request.get_json()
    username = data['username']
    password = data['password']

    status, token = user_login(username, password, global_repo_abs_path)
    # status, token = user_login_orm(username, password, global_database_manager, global_user_model)


    return jsonify({
        'status': status,
        'token': token
    }), 200

@app.route('/api/user/all', methods=['POST'])
def getAllUser():
    ret = get_all_user(global_repo_abs_path)
    # ret = get_all_user_orm(global_database_manager, global_user_model)

    return jsonify({
        'users': ret
    }), 200

@app.route('/api/user/getUserByName', methods=['POST'])
def getUserByName():
    data = request.get_json()
    username = data['username']
    ret = get_user_by_name(username, global_repo_abs_path)
    # ret = get_user_by_name_orm(username, global_database_manager, global_user_model)

    return jsonify({
        'users': ret
    }), 200

@app.route('/api/user/changeUserStatus', methods=['POST'])
def changeUserStatus():
    data = request.get_json()
    username = data['username']
    status = data['status']
    ret = change_user_status(username, status, global_repo_abs_path)
    # ret = change_user_status_orm(username, status, global_database_manager, global_user_model)

    return jsonify({
        'status': ret
    }), 200

@app.route('/api/user/deleteUser', methods=['POST'])
def deleteUser():
    data = request.get_json()
    username = data['username']
    ret = delete_user(username, global_repo_abs_path)
    # ret = delete_user_orm(username, global_database_manager, global_user_model)

    return jsonify({
        'status': ret
    }), 200

@app.route('/api/user/editUser', methods=['POST'])
def editUser():
    data = request.get_json()
    username = data['userName']
    user_type = data['userType']
    email = data['userEmail']
    phone = data['userPhone']
    ret = edit_user(username, user_type, email, phone, global_repo_abs_path)
    # ret = edit_user_orm(username, user_type, email, phone, global_database_manager, global_user_model)

    return jsonify({
        'status': ret
    }), 200

@app.route('/api/user/resetPassword', methods=['POST'])
def resetPassword():
    data = request.get_json()
    username = data['username']
    password = data['password']
    ret = reset_password(username, password, global_repo_abs_path)
    # ret = reset_password_orm(username, password, global_database_manager, global_user_model)
    return jsonify({
        'status': ret
    }), 200
#============user api end================

#============preprocess api start==============
# @app.route('/api/station/preprocess', methods=['GET'])
# def run_process_entry():
#     station_name = request.args.get('station_name')
#     process_date = request.args.get('process_date')

#     result = run_process_manual(station_name, process_date,global_kairosdb_url, global_repo_abs_path, global_time_window, global_database_manager, global_station_models, global_impute_models) # 指定日期
#     return jsonify(result)

#============main api end==============



if __name__ == '__main__':
    app.run(host='0.0.0.0', port=1022, debug=False)

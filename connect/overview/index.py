import json
from flask import jsonify
import os
import random
import logging

# 日志配置（只需在模块顶部配置一次即可）
logger = logging.getLogger(__name__)

def get_repo_abs_path():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # 向上导航到backend目录
    backend_dir = os.path.dirname(os.path.dirname(current_dir))
    return backend_dir

def get_json_file_state(station_name, process_date, repo_abs_path):
    result_path = os.path.join(repo_abs_path, 'data', station_name, 'results', process_date + '.json')
    # result_path = os.path.join(repo_abs_path, 'data', station_name, 'maps', process_date + '.json')
    
    # 直接检查文件是否存在
    return {"json_file_state": os.path.exists(result_path)}

"""
    根据 confidence 值计算颜色（从红色渐变到绿色）。

    参数:
    confidence (int): confidence 值，范围 0-100。

    返回:
    str: 颜色值，格式为 "rgb(R, G, B)"。
"""
def confidence_to_color(confidence):
    # 红色 (255, 0, 0) -> 绿色 (0, 255, 0)
    red = int(255 * (100 - confidence) / 100)
    green = int(255 * confidence / 100)
    blue = 0
    return f"rgb({red}, {green}, {blue})"

def get_middle_color(color1, color2, t): # 返回color1到color2之间的颜色；color1更深；t越小，越接近深色color1
    r = int(color1[0] + (color2[0] - color1[0]) * t)
    g = int(color1[1] + (color2[1] - color1[1]) * t)
    b = int(color1[2] + (color2[2] - color1[2]) * t)
    a = 1  # 保持透明度为 1

    return (r, g, b, a)

def confidence2color_anomaly(confidence, colors, anomaly_type):
    # 根据 confidence 调整字体颜色
    font_color = "#000000" if confidence > 0.5 else "#ffffff"

    if (confidence < 0):
        return "rgba(85, 85, 85, 1)",font_color
    
    color = colors.get(anomaly_type, (85, 85, 85, 1))  # color形如 "rgba(255,140,125,1)" 或默认为 "rgba(170, 170, 170, 1)"

    # 默认颜色和背景色
    default_color = (85, 85, 85, 1)  # 默认颜色 rgba(85,85,85,1)
    target_color = color  # 目标颜色 rgba(255,140,125,1)
    middle_color = get_middle_color(default_color, target_color, 0.3)

    # 插值计算新颜色
    r = int(middle_color[0] + (target_color[0] - middle_color[0]) * confidence)
    g = int(middle_color[1] + (target_color[1] - middle_color[1]) * confidence)
    b = int(middle_color[2] + (target_color[2] - middle_color[2]) * confidence)
    a = middle_color[3]  # 保持透明度为 1

    # 返回调整后的颜色（rgba格式）和字体颜色
    return f"rgba({r}, {g}, {b}, {a})", font_color

def confidence2color_degradation(confidence, color):
    # 根据 confidence 调整字体颜色
    font_color = "#000000" if confidence > 0.5 else "#ffffff"

    if confidence < 0:
        return "rgba(85, 85, 85, 1)", font_color

    # 默认颜色和背景色
    default_color = (85, 85, 85, 1)  # 默认颜色 rgba(85,85,85,1)
    target_color = color  # 目标颜色 rgba(255,140,125,1)
    middle_color = get_middle_color(default_color, target_color, 0.3)


    # 插值计算新颜色
    r = int(middle_color[0] + (target_color[0] - middle_color[0]) * confidence)
    g = int(middle_color[1] + (target_color[1] - middle_color[1]) * confidence)
    b = int(middle_color[2] + (target_color[2] - middle_color[2]) * confidence)
    a = middle_color[3]  # 保持透明度为 1

    # 返回调整后的颜色（rgba格式）和字体颜色
    return f"rgba({r}, {g}, {b}, {a})", font_color


"""
为 GeoJSON 数据增加 confidence 和 color 属性。

参数:
panel_geo_data (dict): 输入的 GeoJSON 数据。

返回:
dict: 包含 confidence 和 color 属性的 GeoJSON 数据。
"""
def add_random_confidence(panel_geo_data):
    # 遍历 GeoJSON 的每个要素
    for feature in panel_geo_data['features']:
        # 生成随机 confidence 值
        confidence = random.randint(0, 100)
        
        # 添加 confidence 属性
        feature['properties']['confidence'] = confidence
        
        # 计算并添加 color 属性
        feature['properties']['color_1'] = confidence_to_color(confidence)

    return panel_geo_data

def set_grey_color(panel_geo_data):
    # 遍历 GeoJSON 的每个要素
    for feature in panel_geo_data['features']:
        # 添加 color 属性
        feature['properties']['anomaly_color'] = '#555'
        feature['properties']['degradation_color'] = '#555'

    return panel_geo_data

def set_mapping_color(panel_geo_data, transformed_result,anoly_max_theshold,color_mappsings):
    # 遍历 GeoJSON 的每个要素
    for feature in panel_geo_data['features']:
        feature_name = feature['properties']['name']
        if feature_name in transformed_result:
            anomaly_score = transformed_result[feature_name].get('anomaly_score', -1)/anoly_max_theshold
            anomaly_type = transformed_result[feature_name].get('anomaly_type', "none")
            degradation_rate = transformed_result[feature_name].get('degradation_rate', -1)
        else:
            anomaly_score = -1
            degradation_rate = -1
            anomaly_type = "none"

        feature['properties']['anomaly_score'] = anomaly_score
        feature['properties']['anomaly_type'] =  anomaly_type # 7种类型之一，或者"none"
        feature['properties']['degradation_rate'] = degradation_rate # 0-1
        # 添加 color 属性
        anomaly_color,anomaly_font_color = confidence2color_anomaly(anomaly_score,color_mappsings['anomaly'],anomaly_type)
        feature['properties']['anomaly_color'] = anomaly_color
        feature['properties']['anomaly_font_color'] = anomaly_font_color
        degradation_color,degradation_font_color = confidence2color_degradation(degradation_rate,color_mappsings['degradation'])
        feature['properties']['degradation_color'] = degradation_color
        feature['properties']['degradation_font_color'] = degradation_font_color

    return panel_geo_data

def set_mapping_color_latest(panel_geo_data, location2string_mapping, result_json_result, color_mappsings):
    # 遍历 GeoJSON 的每个要素
    for feature in panel_geo_data['features']:
        feature_name = feature['properties']['name']
        if feature_name in location2string_mapping:
            string_dict = result_json_result.get(location2string_mapping[feature_name], {})
            diagnosis_results = string_dict.get('diagnosis_results', [])
            if diagnosis_results:
                anomaly_score = diagnosis_results[0].get('rate', -1)
                anomaly_type = diagnosis_results[0].get('result', "none")
                degradation_rate = string_dict.get('degradation_score', -1)

        else:
            anomaly_score = -1
            degradation_rate = -1
            anomaly_type = "none"

        feature['properties']['anomaly_score'] = anomaly_score
        feature['properties']['anomaly_type'] =  anomaly_type # 3种类型之一，或者"none"
        feature['properties']['degradation_rate'] = degradation_rate # 0-1
        # 添加 color 属性
        anomaly_color,anomaly_font_color = confidence2color_anomaly(anomaly_score,color_mappsings['anomaly'],anomaly_type)
        feature['properties']['anomaly_color'] = anomaly_color
        feature['properties']['anomaly_font_color'] = anomaly_font_color
        degradation_color,degradation_font_color = confidence2color_degradation(degradation_rate,color_mappsings['degradation'])
        feature['properties']['degradation_color'] = degradation_color
        feature['properties']['degradation_font_color'] = degradation_font_color

    return panel_geo_data


def transform_result_json(result_json,degradation_dict,anomaly_dict, time_window):
    '''
    result_json形如{
        "date": "2024-12-18",
        "results" : {
            "001-002-032": {
                "location_id": "1-4-5",
                "dpocr": "001-002-032",
                "confidence": 90,
            },
            "002-004-033": {
                "location_id": "7-2-6",
                "dpocr": "001-002-032",
                "confidence": 90,
            },
        }
    }
    本函数的目标是把result_json中results的key值和value中的location_id键的值进行转换，如下：
    {
        "1-4-5": "001-002-032",
        "7-2-6": "002-004-033",
    }
    '''
    transformed_json = {}

    # 确保result_json中包含results键
    if "results" in result_json:
        # 遍历results中的每一项
        for key, value in result_json["results"].items():

            # 确保每一项中包含location_id键
            if "location_id" in value:
                transformed_json[value["location_id"]] = {}
                # 将location_id作为新字典的键，原键作为值
                transformed_json[value["location_id"]]['ID'] = key
                if "anomaly_identifier" in value:
                    if value["anomaly_identifier"] == "normal":
                        if "degradation_rate" in value and value["degradation_rate"] is not None and value["degradation_rate"] > 0:
                            transformed_json[value["location_id"]]['degradation_rate'] = value["degradation_rate"]
                            # 满足条件：value["anomaly_identifier"]为"normal" 且 degradation_rate > 0
                            degradation_dict["低效组串数量"]+=1 
                        else:
                            degradation_dict["正常组串数量"]+=1
                            transformed_json[value["location_id"]]['degradation_rate'] = -1
                    else:
                        degradation_dict["正常组串数量"]+=1
                        transformed_json[value["location_id"]]['anomaly_score'] = -1
                else :
                    transformed_json[value["location_id"]]['degradation_rate'] = -1
                if "anomaly_score" in value and value["anomaly_score"] is not None:
                    transformed_json[value["location_id"]]['anomaly_score'] = value["anomaly_score"]

                    if value["anomaly_score"]/time_window > 0.5:
                        # 检查 "diagnosis_results" 是否存在于 value 中，并且是否为非空数组
                        if "diagnosis_results" in value and isinstance(value["diagnosis_results"], list) and len(value["diagnosis_results"]) > 0:
                            current_type = value["diagnosis_results"][0]["result"]
                            # 判断第一个诊断结果是否在 anomaly_dict的键 中，如果不在，则跳过
                            if current_type not in anomaly_dict:
                                continue
                            transformed_json[value["location_id"]]['anomaly_type'] = current_type
                            anomaly_dict[current_type]+=1
                else:
                    transformed_json[value["location_id"]]['anomaly_score'] = -1

    return transformed_json,degradation_dict,anomaly_dict

def get_result(station_name, process_date,degradation_dict,anomaly_dict, repo_abs_path, time_window):
    result_path = os.path.join(repo_abs_path, 'data',station_name,'results',process_date+'.json')
    # 尝试读取文件
    try:
        with open(result_path, 'r', encoding='utf-8') as result_file:
            result_json = json.load(result_file)
            transformed_result,degradation_dict,anomaly_dict = transform_result_json(result_json,degradation_dict,anomaly_dict, time_window)
            print("degradation_dict: {}".format(degradation_dict))
        return transformed_result,degradation_dict,anomaly_dict
    except FileNotFoundError:
        print(f"Not find file: {result_path}")
        return {},degradation_dict,anomaly_dict
    except Exception as e:
        print(f"Error reading file: {result_path}")
        return {},degradation_dict,anomaly_dict
    
def update_geojson(panel_geo_data,transformed_result,anoly_max_theshold,color_mappsings):
    if not transformed_result:
        panel_geo_data = set_grey_color(panel_geo_data)
        return panel_geo_data
    else:
        panel_geo_data = set_mapping_color(panel_geo_data, transformed_result,anoly_max_theshold,color_mappsings)
        return panel_geo_data

def update_geojson_latest(panel_geo_data,location2string_mapping,result_json_result, color_mappings):
    if not result_json_result:
        panel_geo_data = set_grey_color(panel_geo_data)
        return panel_geo_data
    else:
        panel_geo_data = set_mapping_color_latest(panel_geo_data, location2string_mapping, result_json_result, color_mappings)
        return panel_geo_data
    

def transform_dict2list(statistics_dict):
    result_list = []

    for key, value in statistics_dict.items():
        result_list.append({
            'name': key,
            'value': value
        })
    return result_list

def get_overview_station_map_latest(station_name, process_date, repo_abs_path):
    origin_geo_path = os.path.join(repo_abs_path, 'merge', station_name, 'config', 'geo.json')
    current_geo_path = os.path.join(repo_abs_path, 'data', station_name, 'maps', f'{process_date}.json')
    geo_label_path = os.path.join(repo_abs_path, 'merge', station_name, 'config', 'geo_label.json')

    # 读取geo_label_path对应的文件
    try:
        with open(geo_label_path, 'r', encoding='utf-8') as label_file:
            panel_label_data = json.load(label_file)
    except Exception as e:
        return jsonify({'error': f'无法读取geo_label文件: {e}'}), 500

    # 优先读取current_geo_path，如果文件损坏或无法读取则回退到origin_geo_path
    panel_geo_data = None
    if os.path.exists(current_geo_path):
        try:
            with open(current_geo_path, 'r', encoding='utf-8') as geo_file:
                panel_geo_data = json.load(geo_file)
        except Exception as e:
            # 读取失败，尝试读取原始geo.json
            try:
                with open(origin_geo_path, 'r', encoding='utf-8') as geo_file:
                    panel_geo_data = json.load(geo_file)
            except Exception as e2:
                return jsonify({'error': f'无法读取geo文件: {e2}'}), 500
    else:
        try:
            with open(origin_geo_path, 'r', encoding='utf-8') as geo_file:
                panel_geo_data = json.load(geo_file)
            
            panel_geo_data = set_grey_color(panel_geo_data)
        except Exception as e:
            return jsonify({'error': f'无法读取geo文件: {e}'}), 500

    merged_data = {
        'panel_geo': panel_geo_data,
        'panel_geo_label': panel_label_data
    }

    return jsonify(merged_data)

    
def matches2mapping(matches_json_path):
    with open(matches_json_path, 'r', encoding='utf-8') as matches_file:
        matches_json = json.load(matches_file)

    mapping = dict()
    for item in matches_json:
        mapping[item['gpcode']] = item['dpocr']

    return mapping

def get_overview_station_map(station_name, process_date, repo_abs_path, time_window):
    geo_path = os.path.join(repo_abs_path, 'merge',station_name,'config','geo.json')
    geo_label_path = os.path.join(repo_abs_path, 'merge',station_name,'config','geo_label.json')

    # 这里有一些常量，日后可能要修改
    ANOMALY_TYPES = ["热斑", "二极管故障", "掉串", "积灰", "遮挡", "设备隐患运行", "设备故障停机"]
    ANOMALY_MAX_THRESHOLD = 30 # 这个阈值是基于 detect的 process功能中，设置的 TIME_WINDOW 来决定的，应当与之保持一致
    COLOR_MAPPINGS = {
        "degradation": (255,140,125,1),
        "anomaly": {
            ANOMALY_TYPES[0]: (255,140,125,1),
            ANOMALY_TYPES[1]: (94,225,203,1),
            ANOMALY_TYPES[2]: (255,255,173,1),
            ANOMALY_TYPES[3]: (140,194,232,1),
            ANOMALY_TYPES[4]: (255,198,107,1),
            ANOMALY_TYPES[5]: (209,204,239,1),
            ANOMALY_TYPES[6]: (196,244,115,1)
        }
    }

    degradation_dict = {
        '低效组串数量': 0,
        '正常组串数量': 0
    }
    anomaly_dict = {
        ANOMALY_TYPES[0]:0,
        ANOMALY_TYPES[1]:0,
        ANOMALY_TYPES[2]:0,
        ANOMALY_TYPES[3]:0,
        ANOMALY_TYPES[4]:0,
        ANOMALY_TYPES[5]:0,
        ANOMALY_TYPES[6]:0,
    }

    transformed_result,degradation_dict,anomaly_dict = get_result(station_name, process_date,degradation_dict, anomaly_dict, repo_abs_path, time_window)
    degradation_list = transform_dict2list(degradation_dict)
    anomaly_list = transform_dict2list(anomaly_dict)
    if not transformed_result:
        print("{} 电厂 {} 日期的结果文件不存在".format(station_name, process_date))
    try:
        with open(geo_path, 'r', encoding='utf-8') as geo_file:
            panel_geo_data = json.load(geo_file)

        # panel_geo_data = add_random_confidence(panel_geo_data)
        panel_geo_data = update_geojson(panel_geo_data,transformed_result,ANOMALY_MAX_THRESHOLD,COLOR_MAPPINGS)

        with open(geo_label_path, 'r', encoding='utf-8') as label_file:
            panel_label_data = json.load(label_file)

        merged_data = {
            'panel_geo': panel_geo_data,
            'panel_geo_label': panel_label_data,
            'degradation_info': degradation_list,
            'anomaly_info': anomaly_list
        }

        return jsonify(merged_data)
    except FileNotFoundError:
        print(f"File not found: { geo_path }")
        return jsonify({'error': '文件不存在'}), 404
    except Exception as e:
        print(f"Error reading file: { geo_path }")
        return jsonify({'error': str(e)}), 500

def save_overview_station_map(station_name, process_date, repo_abs_path, time_window):
    geo_path = os.path.join(repo_abs_path, 'merge',station_name,'config','geo.json')
    geo_label_path = os.path.join(repo_abs_path, 'merge',station_name,'config','geo_label.json')

    # 这里有一些常量，日后可能要修改
    ANOMALY_TYPES = ["热斑", "二极管故障", "掉串", "积灰", "遮挡", "设备隐患运行", "设备故障停机"]
    ANOMALY_MAX_THRESHOLD = 30 # 这个阈值是基于 detect的 process功能中，设置的 TIME_WINDOW 来决定的，应当与之保持一致
    COLOR_MAPPINGS = {
        "degradation": (255,140,125,1),
        "anomaly": {
            ANOMALY_TYPES[0]: (255,140,125,1),
            ANOMALY_TYPES[1]: (94,225,203,1),
            ANOMALY_TYPES[2]: (255,255,173,1),
            ANOMALY_TYPES[3]: (140,194,232,1),
            ANOMALY_TYPES[4]: (255,198,107,1),
            ANOMALY_TYPES[5]: (209,204,239,1),
            ANOMALY_TYPES[6]: (196,244,115,1)
        }
    }

    degradation_dict = {
        '低效组串数量': 0,
        '正常组串数量': 0
    }
    anomaly_dict = {
        ANOMALY_TYPES[0]:0,
        ANOMALY_TYPES[1]:0,
        ANOMALY_TYPES[2]:0,
        ANOMALY_TYPES[3]:0,
        ANOMALY_TYPES[4]:0,
        ANOMALY_TYPES[5]:0,
        ANOMALY_TYPES[6]:0,
    }

    transformed_result,degradation_dict,anomaly_dict = get_result(station_name, process_date,degradation_dict, anomaly_dict, repo_abs_path, time_window)
    degradation_list = transform_dict2list(degradation_dict)
    anomaly_list = transform_dict2list(anomaly_dict)
    if not transformed_result:
        print("{} 电厂 {} 日期的结果文件不存在".format(station_name, process_date))
    try:
        with open(geo_path, 'r', encoding='utf-8') as geo_file:
            panel_geo_data = json.load(geo_file)

        # panel_geo_data = add_random_confidence(panel_geo_data)
        panel_geo_data = update_geojson(panel_geo_data,transformed_result,ANOMALY_MAX_THRESHOLD,COLOR_MAPPINGS)

        with open(geo_label_path, 'r', encoding='utf-8') as label_file:
            panel_label_data = json.load(label_file)

        merged_data = {
            'panel_geo': panel_geo_data,
            'panel_geo_label': panel_label_data,
            'degradation_info': degradation_list,
            'anomaly_info': anomaly_list
        }

        # Create directory if it doesn't exist and save the data
        output_dir = os.path.join(repo_abs_path,'data', station_name,'maps')
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, f'{process_date}.json')
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(merged_data, f, ensure_ascii=False, indent=2)

        print("电厂：{} 日期：{} 的地图文件已保存到 {}".format(station_name,process_date,output_path))
    except FileNotFoundError:
        print(f"File not found: { geo_path }")
    except Exception as e:
        print(f"Error reading file: { geo_path }")

def get_overview_station_info(station_name, process_date, repo_abs_path):
    result_json_path = os.path.join(repo_abs_path, 'data', station_name, 'results', f'{process_date}.json')

    degradation_dict = {
        '低效组串数量': 0,
        '正常组串数量': 0
    }

    anomaly_dict = {
        '二极管故障': 0,
        '表面污迹': 0,
        '组串开路或短路': 0,
    }
    power_info = {
        'cumulativePower': 0,
        'monthlyPower': 0,
        'dailyPower': 0
    }
    loss_info = {
        'cumulativeLoss': 0,  
        'dailyLoss': 0, 
        'futureWeekLoss': 0   
    }
    
    if not os.path.exists(result_json_path):
        station_info = {
            'degradation_info': transform_dict2list(degradation_dict),
            'anomaly_info': transform_dict2list(anomaly_dict),
            'power_info': power_info,
            'loss_info': loss_info
        }
        return jsonify(station_info)
    
    # 读取结果文件，增加损坏校验
    try:
        with open(result_json_path, 'r', encoding='utf-8') as result_file:
            result_json = json.load(result_file)
        station_statistics = result_json.get('statistics', {})

        fault_string_count = station_statistics.get('fault_string_count', 0)
        total_strings = station_statistics.get('total_strings', 0)
        degradation_dict = {
            '低效组串数量': fault_string_count,
            '正常组串数量': max(total_strings - fault_string_count, 0)
        }

        anomaly_dict = station_statistics.get('fault_string_dict', {
            '二极管故障': 0,
            '表面污迹': 0,
            '组串开路或短路': 0,
        })

        power_info = {
            'cumulativePower': round(float(station_statistics.get('sum_energy', 0)), 2),
            'monthlyPower': round(float(station_statistics.get('month_energy', 0)), 2),
            'dailyPower': round(float(station_statistics.get('generated_energy', 0)), 2)
        }

        loss_info = {
            'cumulativeLoss': round(float(station_statistics.get('cumulative_loss', 0)), 2),
            'dailyLoss': round(float(station_statistics.get('loss_energy', 0)), 2),
            'futureWeekLoss': round(float(station_statistics.get('future_week_loss', 0)), 2)
        }
    except Exception as e:
        # 文件损坏或无法读取，返回默认结果
        logger.error(f"{result_json_path} 文件损坏或无法读取，返回默认结果: {e}")

    station_info = {
        'degradation_info': transform_dict2list(degradation_dict),
        'anomaly_info': transform_dict2list(anomaly_dict),
        'power_info': power_info,
        'loss_info': loss_info
    }
    return jsonify(station_info)

def get_overview_data(repo_abs_path):
    """
    获取当前的station数据

    Returns:
        json: overview data
    """
    file_path = os.path.join(repo_abs_path,'config', 'overview.json')
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            results_data = json.load(f)
            
        return results_data
    except FileNotFoundError:
        return {}
    except Exception as e:
        print(f"Error get overview data: {str(e)}")
        return {}

if __name__ == '__main__':
    repo_abs_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    station_name = 'datu'
    process_date = '2024-12-02'
    time_window = 30
    save_overview_station_map(station_name, process_date, repo_abs_path, time_window)
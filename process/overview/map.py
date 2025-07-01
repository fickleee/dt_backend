import json
import os
import logging

# 日志配置（只需在模块顶部配置一次即可）
logger = logging.getLogger(__name__)

COLOR_MAPPINGS = {
    "degradation": (255,140,125,1),
    "anomaly": {
        "二极管故障": (255,140,125,1),
        "表面污迹": (94,225,203,1),
        "组串开路或短路": (255,255,173,1)
    }
}

def matches2mapping(matches_json_path):
    with open(matches_json_path, 'r', encoding='utf-8') as matches_file:
        matches_json = json.load(matches_file)

    mapping = dict()
    for item in matches_json:
        mapping[item['gpcode']] = item['dpocr']

    return mapping

def set_grey_color(panel_geo_data):
    # 遍历 GeoJSON 的每个要素
    for feature in panel_geo_data['features']:
        # 添加 color 属性
        feature['properties']['anomaly_color'] = '#555'
        feature['properties']['degradation_color'] = '#555'

    return panel_geo_data

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

def get_middle_color(color1, color2, t): # 返回color1到color2之间的颜色；color1更深；t越小，越接近深色color1
    r = int(color1[0] + (color2[0] - color1[0]) * t)
    g = int(color1[1] + (color2[1] - color1[1]) * t)
    b = int(color1[2] + (color2[2] - color1[2]) * t)
    a = 1  # 保持透明度为 1

    return (r, g, b, a)

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

def set_mapping_color(panel_geo_data, location2string_mapping, daily_results, color_mappings):
    # 遍历 GeoJSON 的每个要素
    for feature in panel_geo_data['features']:
        feature_name = feature['properties']['name']
        if feature_name in location2string_mapping:
            string_dict = daily_results.get(location2string_mapping[feature_name], {})
            diagnosis_results = string_dict.get('diagnosis_results', [])
            if diagnosis_results:
                anomaly_score = diagnosis_results[0].get('rate', -1)
                anomaly_type = diagnosis_results[0].get('result', "none")
                degradation_rate = string_dict.get('degradation_score', -1)
            else:
                anomaly_score = -1
                degradation_rate = -1
                anomaly_type = "none"

        else:
            anomaly_score = -1
            degradation_rate = -1
            anomaly_type = "none"

        # 添加 color 属性
        anomaly_color,anomaly_font_color = confidence2color_anomaly(anomaly_score,color_mappings['anomaly'],anomaly_type)
        print(f"Feature: {feature_name}, Anomaly Score: {anomaly_score}, Anomaly Type: {anomaly_type}, Degradation Rate: {degradation_rate}")
        feature['properties']['anomaly_color'] = anomaly_color
        feature['properties']['anomaly_font_color'] = anomaly_font_color
        degradation_color,degradation_font_color = confidence2color_degradation(degradation_rate,color_mappings['degradation'])
        feature['properties']['degradation_color'] = degradation_color
        feature['properties']['degradation_font_color'] = degradation_font_color

    return panel_geo_data

def update_geojson_latest(panel_geo_data,location2string_mapping,daily_results, color_mappings):
    if not daily_results:
        panel_geo_data = set_grey_color(panel_geo_data)
        return panel_geo_data
    else:
        panel_geo_data = set_mapping_color(panel_geo_data, location2string_mapping, daily_results, color_mappings)
        return panel_geo_data

def generate_map_data(repo_abs_path, station_name, process_date):
    origin_geojson_path = os.path.join(repo_abs_path, 'merge', station_name, 'config', 'geo.json')
    daily_log_path = os.path.join(repo_abs_path, 'data', station_name, 'results', f'{process_date}.json')
    matches_json_path = os.path.join(repo_abs_path, 'merge', station_name, 'config', 'matches.json')
    map_geojson_dir = os.path.join(repo_abs_path, 'data', station_name, 'maps')
    os.makedirs(map_geojson_dir, exist_ok=True)
    map_geojson_path = os.path.join(map_geojson_dir, f'{process_date}.json')

    with open(origin_geojson_path, 'r', encoding='utf-8') as file:
        panel_geo_data = json.load(file)

    # 只有当 daily_log_path对应的文件存在且能够正常读取时才生成地图数据
    if os.path.exists(daily_log_path):
        try:
            with open(daily_log_path, 'r', encoding='utf-8') as file:
                daily_data = json.load(file)
            daily_results = daily_data.get('results', {})

            location2string_mapping = matches2mapping(matches_json_path)

            panel_geo_data = update_geojson_latest(panel_geo_data, location2string_mapping, daily_results, COLOR_MAPPINGS)

            # 保存处理后的 GeoJSON 数据
            with open(map_geojson_path, 'w', encoding='utf-8') as file:
                json.dump(panel_geo_data, file, ensure_ascii=False)
        except Exception as e:
            # 文件损坏或无法读取，全部置灰
            logger.error(f"{daily_log_path} 文件损坏或无法读取，地图全部置灰: {e}")
            panel_geo_data = set_grey_color(panel_geo_data)
            with open(map_geojson_path, 'w', encoding='utf-8') as file:
                json.dump(panel_geo_data, file, ensure_ascii=False)
    else:
        logger.warning(f"{daily_log_path} 文件不存在，地图全部置灰")
        panel_geo_data = set_grey_color(panel_geo_data)
        with open(map_geojson_path, 'w', encoding='utf-8') as file:
            json.dump(panel_geo_data, file, ensure_ascii=False)

import json
from flask import jsonify, send_from_directory, abort
import os

def get_repo_abs_path():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # 向上导航到backend目录
    backend_dir = os.path.dirname(os.path.dirname(current_dir))
    return backend_dir

def get_merge_results(station_name, repo_abs_path):
    results_path = os.path.join(repo_abs_path, 'merge',station_name,'config','results.json')
    # results_path = f'./merge/{station_name}/config/results.json'

    try:
        with open(results_path, 'r', encoding='utf-8') as results_file:
            results = json.load(results_file)

        return jsonify(results)
    except FileNotFoundError:
        abort(404, description=f"Files for station {station_name} not found.")
    except Exception as e:
        abort(500, description=str(e))


def get_merge_map(station_name, repo_abs_path):
    geo_path = os.path.join(repo_abs_path, 'merge',station_name,'config','geo.json')
    geo_label_path = os.path.join(repo_abs_path, 'merge',station_name,'config','geo_label.json')
    # geo_path = f'./merge/{station_name}/config/geo.json'
    # geo_label_path = f'./merge/{station_name}/config/geo_label.json'

    try:
        with open(geo_path, 'r', encoding='utf-8') as geo_file:
            panel_geo_data = json.load(geo_file)

        with open(geo_label_path, 'r', encoding='utf-8') as label_file:
            panel_label_data = json.load(label_file)

        merged_data = {
            'panel_geo': panel_geo_data,
            'panel_geo_label': panel_label_data
        }

        return jsonify(merged_data)
    except FileNotFoundError:
        return jsonify({'error': '文件不存在'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500


def get_merge_image(station_name, filename, repo_abs_path):
    filename_parts = filename.rsplit('_', 1)
    if len(filename_parts) != 2:
        abort(400, description="Invalid filename format")

    filename_dir = filename_parts[0]

    img_path = os.path.join(repo_abs_path, 'merge',station_name,'image',filename_dir)
    # img_path = f'./merge/{station_name}/image/{filename_dir}'

    return send_from_directory(img_path, filename)


def save_merge_data(station_name,match_data,ocr_data, repo_abs_path):
    match_path = os.path.join(repo_abs_path, 'merge',station_name,'config','matches.json')
    result_path = os.path.join(repo_abs_path, 'merge',station_name,'config','results.json')
    # match_path = f'./merge/{station_name}/config/matches.json'  # 1对1，仅修改匹配
    # result_path = f'./merge/{station_name}/config/results.json'  # 1对多，修改匹配和结果

    def modify_data(match_path, result_path, match_data, ocr_data):
        try:
            # 1 修改1对1的matches文件，修改ocr和最佳地理匹配
            with open(match_path, 'r', encoding='utf-8') as match_file:
                match_file_data = json.load(match_file)

                for key, value in match_data.items():
                    match_file_data[int(key) - 1]['gpcode'] = value

                for key, value in ocr_data.items():
                    match_file_data[int(key) - 1]['dpocr'] = value

            # 保存修改后的json
            with open(match_path, 'w', encoding='utf-8') as match_file:
                json.dump(match_file_data, match_file, ensure_ascii=False, indent=4)

            # 2 修改1对多的results文件，修改ocr和最佳地理匹配
            with open(result_path, 'r', encoding='utf-8') as result_file:
                result_file_data = json.load(result_file)

                for key, value in match_data.items():
                    specific_idx = int(key) - 1
                    specific_key = value

                    specific_value = result_file_data[specific_idx]['matched_results'].get(specific_key)

                    # 创建新的字典
                    new_dict = {}
                    if specific_key in result_file_data[specific_idx]['matched_results']:
                        new_dict[specific_key] = specific_value

                    # 添加其他键值对
                    for key, value in result_file_data[specific_idx]['matched_results'].items():
                        if key != specific_key:
                            new_dict[key] = value

                    # 更新 result_data 中的 matched_results
                    result_file_data[specific_idx]['matched_results'] = new_dict

                for key, value in ocr_data.items():
                    result_file_data[int(key) - 1]['dpocr'] = value

            # 保存修改后的json
            with open(result_path, 'w', encoding='utf-8') as result_file:
                json.dump(result_file_data, result_file, ensure_ascii=False, indent=4)

        except Exception as e:
            print(e)

    modify_data(match_path, result_path, match_data, ocr_data)

    # 返回响应
    return jsonify({"message": "Data received successfully"}), 200

if __name__ == '__main__':
    print(get_repo_abs_path())


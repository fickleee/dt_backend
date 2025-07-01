import os
import json
import numpy as np
import pandas as pd

def plot_data_fusion(geo_plot_file, bp_plot_file):
    # 读取地块级的地理数据和图纸数据（txt格式）
    with open(geo_plot_file, 'r') as f:
        geo_data = json.load(f)  # 直接加载为JSON

    with open(bp_plot_file, 'r') as f:
        bp_data = json.load(f)  # 直接加载为JSON

    # # 将JSON数据转换为NumPy数组
    # geo_array = json_to_numpy(geo_data)
    # bp_array = json_to_numpy(bp_data)

    # 将JSON数据转换为Pandas DataFrame
    geo_df = json_to_dataframe(geo_data)
    bp_df = json_to_dataframe(bp_data)

    # 给bp_df增加centerX和centerY列
    # centerX列由dpolx列和dporx列求平均值
    bp_df['centerX'] = (bp_df['dpolx'] + bp_df['dporx']) / 2
    bp_df['centerY'] = (bp_df['dpoly'] + bp_df['dpory']) / 2

    # 给bp_df增加scaleX和scaleY列
    bp_df = convert_df(geo_df, bp_df)

    result = match_geo_to_bp_unique(geo_df, bp_df)



    return result

def json_to_numpy(json_data):
    """将JSON数组转换为NumPy数组，键作为列"""
    # 先转换为Pandas DataFrame
    df = pd.DataFrame(json_data)
    # 再转换为NumPy数组
    return df.to_numpy()

def json_to_dataframe(json_data):
    return pd.DataFrame(json_data)

def convert_df(geo_df, bp_df):
    # 归一化 bp_df 的 centerX 列
    centerX_min = bp_df['centerX'].min()
    centerX_max = bp_df['centerX'].max()
    centerX_normalized = (bp_df['centerX'] - centerX_min) / (centerX_max - centerX_min)

    # 缩放到 geo_df 的 gpccx 的范围
    gpccx_min = geo_df['gpccx'].min()
    gpccx_max = geo_df['gpccx'].max()
    bp_df['scaleX'] = centerX_normalized * (gpccx_max - gpccx_min) + gpccx_min

    # 归一化 bp_df 的 centerY 列
    centerY_min = bp_df['centerY'].min()
    centerY_max = bp_df['centerY'].max()
    centerY_normalized = (bp_df['centerY'] - centerY_min) / (centerY_max - centerY_min)

    # 缩放到 geo_df 的 gpccy 的范围，并反转 Y 轴
    gpccy_min = geo_df['gpccy'].min()
    gpccy_max = geo_df['gpccy'].max()
    bp_df['scaleY'] = centerY_normalized * (gpccy_min - gpccy_max) + gpccy_max

    return bp_df  # 返回修改后的 bp_df

def match_geo_to_bp_unique(geo_df, bp_df):
    """
    地理数据与图纸数据一对一匹配（确保图纸点不被重复匹配）
    使用距离矩阵计算，并转换为0-100的接近度(proximity)

    Args:
        geo_df (pd.DataFrame): 必须包含 gpccx, gpccy, gpcode 列
        bp_df (pd.DataFrame): 必须包含 scaleX, scaleY, dpocr 列

    Returns:
        list: 匹配结果，格式 [{gpcode: {geo_data, bp_data, proximity}}, ...]
    """
    # 提取坐标数据
    geo_coords = geo_df[['gpccx', 'gpccy']].values
    bp_coords = bp_df[['scaleX', 'scaleY']].values

    # 1. 计算所有geo点到所有bp点的距离矩阵
    diff = geo_coords[:, np.newaxis, :] - bp_coords[np.newaxis, :, :]
    distance_matrix = np.sqrt(np.sum(diff ** 2, axis=2))  # 形状: (n_geo, n_bp)

    # 2. 计算接近度(proximity)矩阵 (0-100)
    # 先找到最小和最大距离用于归一化
    min_dist = np.min(distance_matrix)
    max_dist = np.max(distance_matrix)

    # 防止除以零(当所有点重合时)
    if max_dist == min_dist:
        proximity_matrix = np.full_like(distance_matrix, 100)  # 所有点完全接近
    else:
        # 距离越小，接近度越高(100表示最近，0表示最远)
        proximity_matrix = 100 * (1 - (distance_matrix - min_dist) / (max_dist - min_dist))

    # 3. 构建所有可能的匹配对 (geo_idx, bp_idx, proximity)
    matches = []
    n_geo, n_bp = proximity_matrix.shape
    for geo_idx in range(n_geo):
        for bp_idx in range(n_bp):
            matches.append((geo_idx, bp_idx, proximity_matrix[geo_idx, bp_idx]))

    # 4. 按接近度降序排序(优先匹配接近度高的对)
    matches.sort(key=lambda x: -x[2])  # 注意这里是降序排序

    # 5. 执行一对一匹配（确保 bp 点不重复）
    matched_geo = set()
    matched_bp = set()
    result = []

    for geo_idx, bp_idx, proximity in matches:
        if geo_idx not in matched_geo and bp_idx not in matched_bp:
            # 记录匹配
            geo_row = geo_df.iloc[geo_idx]
            bp_row = bp_df.iloc[bp_idx]
            # result.append({
            #     geo_row['gpcode']: {
            #         'geo_data': geo_row.to_dict(),
            #         'bp_data': bp_row.to_dict(),
            #         'proximity': round(proximity, 2)  # 保留2位小数
            #     }
            # })
            result.append({
                'dpocr': bp_row['dpocr'],
                'dpclx': bp_row['dpclx'],
                'dpcly': bp_row['dpcly'],
                'dpcrx': bp_row['dpcrx'],
                'dpcry': bp_row['dpcry'],
                'dp_img_name': bp_row['dp_img_name'],
                'matched_results': {
                    f"{geo_row['gpcode']}": {
                        "proximity":  round(proximity, 2),
                        "gpocx": geo_row['gpocx'],
                        "gpocy": geo_row['gpocy']
                    }
                }
            })
            # 标记已匹配
            matched_geo.add(geo_idx)
            matched_bp.add(bp_idx)
            # 如果所有点都已匹配，提前退出
            if len(matched_geo) == n_geo or len(matched_bp) == n_bp:
                break

    return result


def format_number_string(input_str):
    """
    将形如 "1-2-3" 的字符串转换为 "001-002-003" 格式
    若输入不符合数字用连字符连接的格式，则返回原字符串

    Args:
        input_str (str): 输入字符串

    Returns:
        str: 格式化后的字符串或原字符串
    """
    parts = input_str.split('-')

    # 检查所有部分是否都是数字
    if all(part.strip().isdigit() for part in parts):
        try:
            # 将每个数字部分格式化为3位数，前面补零
            formatted_parts = [f"{int(part):03d}" for part in parts]
            return '-'.join(formatted_parts)
        except ValueError:
            # 处理意外情况（虽然理论上不会发生，因为前面已经用isdigit检查过）
            return input_str
    else:
        # 如果有非数字部分，返回原字符串
        return input_str


def data_fusion(repo_abs_path, station_name):
    geo_dir_path = os.path.join(repo_abs_path, 'merge', station_name, 'plot_label', 'geo')
    blueprint_dir_path = os.path.join(repo_abs_path, 'merge', station_name, 'plot_label', 'blueprint')

    fusion_list = []

    # 已知geo_dir_path和 blueprint_dir_path 应当具有相同的文件，如1.json, 2.json等
    # 遍历geo_dir_path
    for file_name in os.listdir(geo_dir_path):
        geo_file_path = os.path.join(geo_dir_path, file_name)
        # 判断 blueprint_dir_path 下是否有同名文件
        blueprint_file_path = os.path.join(blueprint_dir_path, file_name)
        if not os.path.exists(blueprint_file_path):
            print(f"warning: {file_name} 在 blueprint_dir_path 中不存在！")
            continue

        plot_fusion_list = plot_data_fusion(geo_file_path, blueprint_file_path)
        # 将plot_fusion_list中的所有元素添加到 fusion_list 中
        fusion_list.extend(plot_fusion_list)

    # 遍历 fusion_list 中的每个元素（字典），给每个元素增加merge_id（从1开始）
    for i, plot_fusion in enumerate(fusion_list):
        plot_fusion['merge_id'] = i + 1

    # 保存到 results.json
    save_results_path = os.path.join(repo_abs_path, 'merge',station_name,'config', 'results.json')
    with open(save_results_path, 'w') as f:
        json.dump(fusion_list, f, indent=4)

    # 对于数组的每个元素，仅保留mergd_id，dpocr和gpcode，然后再保存到matches.json
    matches_list = [{'merge_id': plot_fusion['merge_id'], 'dpocr': format_number_string(plot_fusion['dpocr']), 'gpcode': next(iter(plot_fusion['matched_results']))} for plot_fusion in fusion_list]
    save_matches_path = os.path.join(repo_abs_path, 'merge',station_name,'config', 'matches.json')
    with open(save_matches_path, 'w') as f:
        json.dump(matches_list, f, indent=4)




if __name__ == '__main__':
    geo_dir_path = 'E:/STUDY/Python/DATANG/PAPER/data-process-reset/datasets/STATIONs/datu/geo/temp'
    blueprint_dir_path = 'E:/STUDY/Python/DATANG/PAPER/data-process-reset/datasets/STATIONs/datu/label/temp'
    results_dir = 'E:/STUDY/Python/DATANG/PAPER/data-process-reset/datasets/STATIONs/datu/results'

    fusion_list = []

    # 已知geo_dir_path和 blueprint_dir_path 应当具有相同的文件，如1.json, 2.json等
    # 遍历geo_dir_path
    for file_name in os.listdir(geo_dir_path):
        geo_file_path = os.path.join(geo_dir_path, file_name)
        # 判断 blueprint_dir_path 下是否有同名文件
        blueprint_file_path = os.path.join(blueprint_dir_path, file_name)
        if not os.path.exists(blueprint_file_path):
            print(f"warning: {file_name} 在 blueprint_dir_path 中不存在！")
            continue

        plot_fusion_list = plot_data_fusion(geo_file_path, blueprint_file_path)
        # 将plot_fusion_list中的所有元素添加到 fusion_list 中
        fusion_list.extend(plot_fusion_list)

    # 遍历 fusion_list 中的每个元素（字典），给每个元素增加merge_id（从1开始）
    for i, plot_fusion in enumerate(fusion_list):
        plot_fusion['merge_id'] = i + 1

    # 保存到 results.json
    save_results_path = os.path.join(results_dir, 'results.json')
    with open(save_results_path, 'w') as f:
        json.dump(fusion_list, f, indent=4)

    # 对于数组的每个元素，仅保留mergd_id，dpocr和gpcode，然后再保存到matches.json
    # matches_list = [{'merge_id': plot_fusion['merge_id'], 'dpocr': format_number_string(plot_fusion['dpocr']), 'gpcode': next(iter(plot_fusion['matched_results']))} for plot_fusion in fusion_list]
    matches_list = [{'merge_id': plot_fusion['merge_id'], 'dpocr': plot_fusion['dpocr'], 'gpcode': next(iter(plot_fusion['matched_results']))} for plot_fusion in fusion_list]
    save_matches_path = os.path.join(results_dir, 'matches.json')
    with open(save_matches_path, 'w') as f:
        json.dump(matches_list, f, indent=4)
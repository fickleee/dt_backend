import os
import json
import pyproj

# 创建一个Proj对象，将经纬度转换为平面坐标
proj = pyproj.Proj(proj='merc', ellps='WGS84')

def is_in_block(center_x, center_y, block_points):
    if not block_points or not block_points[0]:
        return False

    # 提取第一层多边形（假设只有单个多边形）
    polygon = block_points[0]
    n = len(polygon)
    inside = False

    # 射线法实现
    x, y = center_x, center_y
    p1x, p1y = polygon[0]
    for i in range(n + 1):
        p2x, p2y = polygon[i % n]
        if y > min(p1y, p2y):
            if y <= max(p1y, p2y):
                if x <= max(p1x, p2x):
                    if p1y != p2y:
                        xinters = (y - p1y) * (p2x - p1x) / (p2y - p1y) + p1x
                    if p1x == p2x or x <= xinters:
                        inside = not inside
        p1x, p1y = p2x, p2y

    return inside

def split_geo_block(geo_json, block_points, output_path):
    lines_list = []

    geo_points_list = geo_json.get('features', [])
    for point in geo_points_list:
        point_coordinates = point['geometry']['coordinates'][0]
        center_loog = (point_coordinates[0][0] + point_coordinates[2][0])/2
        center_lat = (point_coordinates[0][1] + point_coordinates[2][1])/2
        point_id = point['properties']['name']
        if is_in_block(center_loog, center_lat, block_points):
            line_dict = dict()
            line_dict['gpocx'] = center_loog
            line_dict['gpocy'] = center_lat

            # 正向投影
            gpccx, gpccy = proj(line_dict['gpocx'], line_dict['gpocy'])

            line_dict['gpccx'] = gpccx
            line_dict['gpccy'] = gpccy

            line_dict['gpcode'] = point_id
            lines_list.append(line_dict)

    # 保存到output_path中，已知output_path 形如 './1.txt'，保留标题行，分隔符为空格
    if lines_list:
        # 定义输出字段顺序
        field_order = ['gpocx', 'gpocy', 'gpccx', 'gpccy', 'gpcode']

        # 写入文件
        with open(output_path, 'w') as f:
            # 写入数据行
            for item in lines_list:
                line = '\t'.join(str(item[field]) for field in field_order)
                f.write(line + '\n')

def split_geo_block_json(geo_json, block_points, output_path):
    lines_list = []

    geo_points_list = geo_json.get('features', [])
    for point in geo_points_list:
        point_coordinates = point['geometry']['coordinates'][0]
        center_loog = (point_coordinates[0][0] + point_coordinates[2][0])/2
        center_lat = (point_coordinates[0][1] + point_coordinates[2][1])/2
        point_id = point['properties']['name']
        if is_in_block(center_loog, center_lat, block_points):
            line_dict = dict()
            line_dict['gpocx'] = center_loog
            line_dict['gpocy'] = center_lat

            # 正向投影
            gpccx, gpccy = proj(line_dict['gpocx'], line_dict['gpocy'])

            line_dict['gpccx'] = gpccx
            line_dict['gpccy'] = gpccy

            line_dict['gpcode'] = point_id
            lines_list.append(line_dict)

    # 保存到output_path中，改为保存为JSON文件
    if lines_list:
        with open(output_path, 'w') as f:
            json.dump(lines_list, f, indent=4)


def split_geo_plot(repo_abs_path, station_name, split_plots):
    geo_json_path = os.path.join(repo_abs_path, 'merge', station_name, 'config', 'geo.json')
    output_labels_dir = os.path.join(repo_abs_path, 'merge', station_name, 'plot_label', 'geo')
    os.makedirs(output_labels_dir, exist_ok=True)

    # 读取json文件
    with open(geo_json_path, 'r') as f:
        geo_json = json.load(f)

    for block_info in split_plots:
        block_name = block_info['name']
        block_points = block_info['points']
        output_path = os.path.join(output_labels_dir, f'{block_name}.json')

        # split_geo_block(geo_json, block_points, output_path)
        split_geo_block_json(geo_json, block_points, output_path)
        print(f"{block_name} 地块分割完成")

if __name__ == "__main__":
    geo_split_file = 'E:/STUDY/Python/DATANG/PAPER/data-process-reset/datasets/STATIONs/datu/geo/split.json'
    # 读取json文件
    with open(geo_split_file, 'r') as f:
        split_plots = json.load(f)

    geo_json_path = 'E:/STUDY/Python/DATANG/PAPER/data-process-reset/datasets/STATIONs/datu/geo/geo.json'
    output_labels_dir = 'E:/STUDY/Python/DATANG/PAPER/data-process-reset/datasets/STATIONs/datu/geo/temp'
    os.makedirs(output_labels_dir, exist_ok=True)

    # 读取geo.json文件
    with open(geo_json_path, 'r') as f:
        geo_json = json.load(f)

    for block_info in split_plots:
        block_name = block_info['name']
        block_points = block_info['points']
        output_path = os.path.join(output_labels_dir, f'{block_name}.json')

        # split_geo_block(geo_json, block_points, output_path)
        split_geo_block_json(geo_json, block_points, output_path)
        print(f"{block_name} 地块分割完成")

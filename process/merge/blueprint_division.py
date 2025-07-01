import os
import json
import re

def is_in_block(center_x, center_y, block_points):
    """
    判断点(center_x, center_y)是否在由block_points定义的多边形内部
    使用射线法（Ray-Casting Algorithm）

    参数:
        center_x: 点的x坐标
        center_y: 点的y坐标
        block_points: 多边形顶点列表，格式为[{"x":x1, "y":y1}, {"x":x2, "y":y2}, ...]

    返回:
        bool: 点在多边形内返回True，否则返回False
    """
    n = len(block_points)
    inside = False

    # 将字典列表转换为坐标元组列表
    polygon = [(p['imageX'], p['imageY']) for p in block_points]

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


def split_bp_block(label_path, block_points, output_path):
    """
    根据多边形区域分割标签文件

    参数:
        label_path: 原始标签文件路径
        block_points: 多边形顶点列表
        output_path: 输出文件路径
    """
    with open(label_path, 'r') as f:
        lines = f.readlines()

    split_lines = []
    for line in lines:
        parts = line.strip().split()
        # 保证每行有10个元素，分别代表dpolx,dpoly,dporx,dpory,dpclx,dpcly,dpcrx,dpcry,dp_img_name,dpocr

        if len(parts) < 4:  # 确保至少有4个坐标值
            continue

        try:
            x1, y1, x2, y2 = map(float, parts[:4])
            center_x, center_y = (x1 + x2) / 2, (y1 + y2) / 2
            if is_in_block(center_x, center_y, block_points):
                cropped_image_prefix = parts[8] # 形如 "tangjing_1_167"，我需要添加".jpg"后缀
                cropped_image_name = cropped_image_prefix + ".jpg"
                parts[8] = cropped_image_name # 对 dp_img_name 进行修改
                split_lines.append('\t'.join(parts) + '\n')
        except ValueError:
            continue

    with open(output_path, 'w') as f:
        f.writelines(split_lines)


def split_bp_block_json(label_path, block_points, output_path):
    """
    根据多边形区域分割标签文件

    参数:
        label_path: 原始标签文件路径
        block_points: 多边形顶点列表
        output_path: 输出文件路径
    """
    with open(label_path, 'r') as f:
        lines = f.readlines()

    split_lines = []
    for line in lines:
        parts = line.strip().split()
        # 保证每行有10个元素，分别代表dpolx,dpoly,dporx,dpory,dpclx,dpcly,dpcrx,dpcry,dp_img_name,dpocr

        if len(parts) < 4:  # 确保至少有4个坐标值
            continue

        try:
            x1, y1, x2, y2 = map(float, parts[:4])
            center_x, center_y = (x1 + x2) / 2, (y1 + y2) / 2
            if is_in_block(center_x, center_y, block_points):
                cropped_image_prefix = parts[8]  # 形如 "tangjing_1_167"
                cropped_image_name = cropped_image_prefix + ".jpg"

                # 构建字典对象
                line_dict = {
                    "dpolx": x1,
                    "dpoly": y1,
                    "dporx": x2,
                    "dpory": y2,
                    "dpclx": float(parts[4]) if len(parts) > 4 else None,
                    "dpcly": float(parts[5]) if len(parts) > 5 else None,
                    "dpcrx": float(parts[6]) if len(parts) > 6 else None,
                    "dpcry": float(parts[7]) if len(parts) > 7 else None,
                    "dp_img_name": cropped_image_name,
                    "dpocr": parts[9] if len(parts) > 9 else None
                }
                split_lines.append(line_dict)
        except ValueError:
            continue

    # 保存到output_path中，改为保存为JSON文件
    if split_lines:
        with open(output_path, 'w') as f:
            json.dump(split_lines, f, indent=4)

def temp_split_bp_block_json(label_path, block_points, output_path):
    """
    根据多边形区域分割标签文件

    参数:
        label_path: 原始标签文件路径
        block_points: 多边形顶点列表
        output_path: 输出文件路径
    """
    with open(label_path, 'r') as f:
        lines = f.readlines()

    split_lines = []
    for line in lines:
        parts = line.strip().split()
        # 保证每行有10个元素，分别代表dpolx,dpoly,dporx,dpory,dpclx,dpcly,dpcrx,dpcry,dp_img_name,dpocr

        if len(parts) < 4:  # 确保至少有4个坐标值
            continue

        try:
            x1, y1, x2, y2 = map(float, parts[:4])
            center_x, center_y = (x1 + x2) / 2, (y1 + y2) / 2
            # if is_in_block(center_x, center_y, block_points):
            cropped_image_prefix = parts[8]  # 形如 "tangjing_1_167"
            cropped_image_name = cropped_image_prefix + ".jpg"

            # 构建字典对象
            line_dict = {
                "dpolx": x1,
                "dpoly": y1,
                "dporx": x2,
                "dpory": y2,
                "dpclx": float(parts[4]) if len(parts) > 4 else None,
                "dpcly": float(parts[5]) if len(parts) > 5 else None,
                "dpcrx": float(parts[6]) if len(parts) > 6 else None,
                "dpcry": float(parts[7]) if len(parts) > 7 else None,
                "dp_img_name": cropped_image_name,
                "dpocr": parts[9] if len(parts) > 9 else None
            }
            split_lines.append(line_dict)
        except ValueError:
            continue

    # 保存到output_path中，改为保存为JSON文件
    if split_lines:
        with open(output_path, 'w') as f:
            json.dump(split_lines, f, indent=4)


def split_blueprint_plot(repo_abs_path, station_name, split_plots):
    origin_labels_dir = os.path.join(repo_abs_path, 'merge', station_name, 'merged_label')
    output_labels_dir = os.path.join(repo_abs_path, 'merge', station_name, 'plot_label', 'blueprint')
    os.makedirs(output_labels_dir, exist_ok=True)

    for plot in split_plots:
        file_name = plot['fileName'] # 形如 'tangjing_1.jpg'
        # 将file_name的后缀名替换成txt
        file_name = re.sub(r'\.\w+$', '.txt', file_name)
        plot_name = plot['plotName'] # 形如 "1"
        points = plot['points'] # 形如 [{canvasX: -243.625, canvasY: -143.5, imageX: 975, imageY: 1572}, {canvasX: -147.625, canvasY: -138.5, imageX: 3911, imageY: 1725}]

        origin_label_path = os.path.join(origin_labels_dir, file_name)
        plot_label_path = os.path.join(output_labels_dir, f"{plot_name}.json")
        # split_bp_block(origin_label_path, points, plot_label_path)
        split_bp_block_json(origin_label_path, points, plot_label_path)
        print(f"{plot_name} 地块分割完成，来自 {file_name}")

if __name__ == "__main__":
    labels_dir = 'E:/STUDY/Python/DATANG/PAPER/data-process-reset/datasets/STATIONs/datu/label/masked_ocr2string'
    output_dir = 'E:/STUDY/Python/DATANG/PAPER/data-process-reset/datasets/STATIONs/datu/label/temp'
    os.makedirs(output_dir, exist_ok=True)

    for file_name in os.listdir(labels_dir):
        if file_name.endswith('.txt'):
            label_path = os.path.join(labels_dir, file_name)
            output_path = os.path.join(output_dir, file_name)
            # output_path的后缀名改为json
            output_path = re.sub(r'\.txt$', '.json', output_path)
            block_points = [
                {"imageX": 100, "imageY": 200},
                {"imageX": 300, "imageY": 400},
                {"imageX": 500, "imageY": 600}
            ]
            temp_split_bp_block_json(label_path, block_points, output_path)
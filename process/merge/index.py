from datetime import datetime
import sqlite3 as sql
from datetime import datetime, timedelta
import time
import json
import os
# import fitz  # PyMuPDF
# import cv2
import numpy as np
import logging
# from process.merge.seg import segment_image
# from process.merge.predict import predict_image
# from process.merge.concat import merge_image
# from process.merge.geo_division import split_geo_plot
# from process.merge.blueprint_division import split_blueprint_plot

# 日志配置（只需在模块顶部配置一次即可）
logger = logging.getLogger(__name__)

# ================ merge =================
'''
    1. 假设 process_date 的格式，形如"2024-12-18"
    2. station_name 为电厂的拼音，形如"datu"等
    3. database_path 能被直接读取，形如"../../database/test.db"
    4. data_dir_path 为data文件夹的相对路径，形如"../../data/"
'''
def create_log(process_date, station_name, database_path, data_dir_path,start_timestamp,end_timestamp):
    # 连接数据库
    conn = sql.connect(database_path)
    cursor = conn.cursor()

    # 查询StringInfo表
    query_string_info = f'''
    SELECT device_id,box_id,inverter_id,string_id FROM {station_name}StringInfo
    WHERE timestamp between ? and ?
    '''
    # 执行查询
    string_info_data = cursor.execute(query_string_info, (start_timestamp, end_timestamp))
    string_info_rows = string_info_data.fetchall()

    # 检查是否有数据返回
    if not string_info_rows:
        print("警告: 创建{}电厂的{}.json 时，未从本地数据库中查询到有效数据！本次创建的日志文件为空！".format(station_name, process_date))

    # 关闭Cursor和Connection
    cursor.close()
    conn.close()

    # 加载当天的数据
    initial_data = get_initial_data(process_date, string_info_rows)

    # 创建data目录下{station_name}/results/{process_date}.json文件并写入数据
    json_dir_path = os.path.join(data_dir_path, station_name, "results")
    os.makedirs(json_dir_path, exist_ok=True)
    json_path = os.path.join(json_dir_path, f'{process_date}.json')
    with open(json_path, 'w') as json_file:
        json.dump(initial_data, json_file, indent=4)

"""
    1. 假设 process_date 的格式，形如"2024-12-18"
    2. station_name 为电厂的拼音，形如"datu"等
    3. data_dir_path 为data文件夹的相对路径，形如"../../data/"
    4. merge_dir_path 为 merge 文件夹的相对路径，形如"../../merge/"
"""
def convert_string_number(string_number):  # string_number形如"001-002-032"，需转换成"1-2-32"
    # 按 '-' 分割字符串
    parts = string_number.split('-')
    # 将每个部分转换为整数，去除前导零
    converted_parts = [str(int(part)) for part in parts]
    # 将转换后的部分重新组合成字符串
    converted_string = '-'.join(converted_parts)
    return converted_string

def transform_datu_string(gpcode, station_name): 
    if station_name == "datu":
        # 原始 gpcode 形如 "1,2,3,4"，取最后一个 ','前的部分
        parts = gpcode.split(',')
        if len(parts) > 1:
            # 取最后一个部分之前的所有部分
            transformed_gpcode = ','.join(parts[:-1])
            return transformed_gpcode
    return gpcode  # 普通场站为 "1,2,3" 形式，直接返回

def merge_log(process_date, station_name, data_dir_path, merge_dir_path):
    origin_json_path = os.path.join(data_dir_path, station_name, "results", f'{process_date}.json')
    merge_json_path = os.path.join(merge_dir_path, station_name, "config", "matches.json")
    nan_location_id_count = 0

    try:
        # 读取origin_json_path文件
        with open(origin_json_path, 'r', encoding='utf-8') as origin_file:
            origin_data = json.load(origin_file)

        # 读取merge_json_path文件并转换为字典
        with open(merge_json_path, 'r', encoding='utf-8') as merge_file:
            merge_data = json.load(merge_file)

        dpocr_gpcode_dict = {item['dpocr']: transform_datu_string(item['gpcode'], station_name) for item in merge_data}

        # 对origin_data["results"]中的每个item进行处理
        for key, value in origin_data["results"].items():
            # converted_key = convert_string_number(key)
            # 修改后的matches文件，其dpocr已经为"001-001-001"的形式
            if key in dpocr_gpcode_dict:
                origin_data["results"][key]["location_id"] = dpocr_gpcode_dict[key]
            else:
                # 如果key不在字典中，则将value["location_id "]设置为"0,0,0"
                origin_data["results"][key]["location_id"] = "0,0,0"
                nan_location_id_count += 1

        # 保存修改后的数据
        with open(origin_json_path, 'w', encoding='utf-8') as modified_file:
            json.dump(origin_data, modified_file, ensure_ascii=False)

    except FileNotFoundError as e:
        print(f"File not found: {e}")
    except json.JSONDecodeError as e:
        print(f"JSON decode error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")


def get_timestamps(process_date):
    # 解析输入的日期字符串为 datetime 对象
    date_obj = datetime.strptime(process_date, '%Y-%m-%d')

    # 获取当天0点的时间戳
    start_timestamp = int(time.mktime(date_obj.timetuple()))

    # 获取第二天0点的时间戳
    end_timestamp = int(time.mktime((date_obj + timedelta(days=1)).timetuple()))

    return start_timestamp, end_timestamp


def get_initial_data(process_date, string_info_rows):
    initial_data = {"date": process_date, "results": {}}
    for row in string_info_rows:
        device_id, box_id, inverter_id, string_id = row
        initial_data["results"][device_id] = {
            "string_id": string_id,
            "inverter_id": inverter_id,
            "box_id": box_id,
        }

    return initial_data

# # ================ merge-create =================
# def mc_pdf2jpg(repo_abs_path, station_name):
#     pdfs_dir = os.path.join(repo_abs_path, 'merge', station_name, 'pdfs')
#     imgs_dir = os.path.join(repo_abs_path, 'merge', station_name, 'imgs')
#     os.makedirs(imgs_dir, exist_ok=True)
#     image_count = 1

#     images_list = []

#     # 遍历PDF文件目录下的所有PDF文件
#     for pdf_file in os.listdir(pdfs_dir):
#         if pdf_file.endswith('.pdf'):
#             pdf_path = os.path.join(pdfs_dir, pdf_file)

#             # 打开PDF文件
#             doc = fitz.open(pdf_path)

#             # 遍历PDF中的每一页
#             for page_num in range(len(doc)):
#                 page = doc.load_page(page_num)  # 加载当前页

#                 # 设置较高的缩放比例，提升分辨率
#                 zoom_factor = 5  # 调整该值以提高分辨率 (越大图像越清晰)
#                 mat = fitz.Matrix(zoom_factor, zoom_factor)

#                 # 渲染页面为高分辨率图像
#                 pix = page.get_pixmap(matrix=mat, alpha=False)  # alpha=False 生成RGB格式图像

#                 # 将图片保存为高分辨率的 JPG 格式
#                 image_name = f"{station_name}_{image_count}.jpg"
#                 images_list.append(image_name)
#                 output_image = os.path.join(imgs_dir, image_name)

#                 image_count += 1

#                 # 使用OpenCV替代Pillow
#                 # 将pix.samples转换为numpy数组
#                 img_array = np.frombuffer(pix.samples, dtype=np.uint8)
#                 # 重塑数组为正确的形状 (height, width, channels)
#                 img_array = img_array.reshape((pix.height, pix.width, 3))
#                 # OpenCV使用BGR格式，所以需要从RGB转换
#                 img_array = cv2.cvtColor(img_array, cv2.COLOR_RGB2BGR)
#                 # 保存图像
#                 cv2.imwrite(output_image, img_array, [int(cv2.IMWRITE_JPEG_QUALITY), 95])

#                 print(f"Page {page_num + 1} saved as {output_image} with resolution {pix.width}x{pix.height}")

#     return images_list

# def get_mc_image_path(repo_abs_path, station_name, image_name):
#     image_path = os.path.join(repo_abs_path, 'merge', station_name, 'imgs', image_name)
#     return image_path

# def get_merged_image_path(repo_abs_path, station_name, image_name):
#     merged_image_path = os.path.join(repo_abs_path, 'merge', station_name, 'merged_image', image_name)
#     return merged_image_path

# def get_merged_label_path(repo_abs_path, station_name, label_name):
#     merged_label_path = os.path.join(repo_abs_path, 'merge', station_name, 'merged_label', label_name)
#     return merged_label_path

# def get_mc_geo_data_path(repo_abs_path, station_name):
#     geo_path = os.path.join(repo_abs_path, 'merge', station_name, 'config', 'geo.json')
#     geo_label_path = os.path.join(repo_abs_path, 'merge', station_name, 'config', 'geo_label.json')
#     return geo_path, geo_label_path

# def process_mc_image(repo_abs_path, station_name, image_name, rotation, rectangles):
#     image_path = get_mc_image_path(repo_abs_path, station_name, image_name)
#     image = cv2.imread(image_path)
#     if image is None:
#         raise ValueError(f"无法读取图片: {image_path}")

#     # 步骤一：添加白色遮罩
#     for rect in rectangles:
#         x, y, w, h = int(rect['x']), int(rect['y']), int(rect['width']), int(rect['height'])
#         # 约束矩形在图片范围内
#         x1, y1 = max(0, x), max(0, y)
#         x2, y2 = min(image.shape[1], x + w), min(image.shape[0], y + h)
#         image[y1:y2, x1:x2] = (255, 255, 255)  # BGR白色

#     # 步骤二：固定角度旋转（0°、90°、180°、270°）
#     if rotation in {90, 180, 270}:
#         # 根据角度选择旋转方式
#         if rotation == 90:
#             image = cv2.rotate(image, cv2.ROTATE_90_CLOCKWISE)
#         elif rotation == 180:
#             image = cv2.rotate(image, cv2.ROTATE_180)
#         elif rotation == 270:
#             image = cv2.rotate(image, cv2.ROTATE_90_COUNTERCLOCKWISE)  # 270°=逆时针90°

#     # 步骤三：保存图片
#     cv2.imwrite(image_path, image)
#     return image_path

# def seg_predict_merge(repo_abs_path, station_name):
#     logger.info("步骤一：图片分割开始")
#     segment_image(repo_abs_path, station_name)
#     logger.info("步骤一结束，步骤二：模型预测开始")
#     predict_image(repo_abs_path, station_name)
#     logger.info("步骤二结束，步骤三：图片合并开始")
#     merge_image(repo_abs_path, station_name)
#     logger.info("步骤三结束")

# def split_plot(repo_abs_path, station_name, split_bp_plots, split_geo_plots):
#     print("图纸分割信息：{}".format(split_bp_plots))
#     print("地理分割信息：{}".format(split_geo_plots))

#     print("开始分割图纸数据")
#     split_blueprint_plot(repo_abs_path, station_name, split_bp_plots)
#     print("开始分割地理数据")
#     split_geo_plot(repo_abs_path, station_name, split_geo_plots)


if __name__ == '__main__':
    PROCESS_DATE = "2024-10-31"
    STATION_NAME = "datu"

    repo_abs_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    DATA_DIR_PATH = os.path.join(repo_abs_path, "data")
    MERGE_DIR_PATH = os.path.join(repo_abs_path, "merge")


    # create_log(PROCESS_DATE, STATION_NAME, DATABASE_PATH, REPO_PATH)
    merge_log(PROCESS_DATE, STATION_NAME, DATA_DIR_PATH, MERGE_DIR_PATH)

from ultralytics import YOLO
import cv2
from paddleocr import PaddleOCR
import numpy as np
import os
import torch

def single_image_OCR(image,OCR):
    result = OCR.ocr(image, cls=True)

    for idx in range(len(result)):
        res = result[idx]
        if res is None:
            return "???"
        else:
            return res[0][1][0]

# 获取每个预测框的数据
def get_box_data(r, test_image, ocr):
    cur_image_data = []

    # 提取边界框（xyxy格式）
    boxes = r.obb.xyxy
    for j, box in enumerate(boxes):
        cur_box_data = []

        # 将边界框坐标转换为整数
        x1, y1, x2, y2 = map(int, box)

        # 因为原图大小为1024*1024，假设中心点坐标的横坐标和纵坐标都介于100-900之间，则认为该边界框有效
        center_x = (x1 + x2) / 2
        center_y = (y1 + y2) / 2
        if center_x >= 100 and center_x <= 900 and center_y >= 100 and center_y <= 900:
            cur_box_data.append([x1, y1])
            cur_box_data.append([x2, y2])

            # 使用边界框坐标裁剪图像
            cropped_image = test_image[y1:y2, x1:x2]

            # 获取裁剪图像的大小
            cropped_height, cropped_width = cropped_image.shape[:2]

            # 创建一个大小为512x512的白色背景图像
            background = np.ones((512, 512, 3), dtype=np.uint8) * 255

            # 计算裁剪图像居中放置的左上角坐标
            x_offset = (512 - cropped_width) // 2
            y_offset = (512 - cropped_height) // 2

            # 将裁剪图像放置到白色背景上
            background[y_offset:y_offset + cropped_height, x_offset:x_offset + cropped_width] = cropped_image

            # 对裁剪后的图像进行OCR识别
            cur_box_ocr = single_image_OCR(background, ocr)
            # cur_box_ocr = optimize_text_recognition_str(cur_box_ocr)  # 优化文本识别结果
            cur_box_data.append(cur_box_ocr)
            cur_image_data.append(cur_box_data)
        # else:
        #     print("警告，无效边界框：x1: {} y1: {} x2: {} y2: {}".format(x1, y1, x2, y2))

    return cur_image_data

def predict_image(repo_abs_path, station_name):
    cropped_all_images_dir = os.path.join(repo_abs_path, "merge",station_name, "image")
    cropped_all_labels_dir = os.path.join(repo_abs_path, "merge", station_name, "cropped_label")
    os.makedirs(cropped_all_labels_dir, exist_ok=True)


    yolo_model_path = os.path.join(repo_abs_path, "process", "merge", "model", "yolo11", "11n.pt")
    paddle_base_path = os.path.join(repo_abs_path, "process", "merge", "model", "paddle")
    paddle_cls_path = os.path.join(paddle_base_path, "cls")
    paddle_det_path = os.path.join(paddle_base_path, "det")
    paddle_rec_path = os.path.join(paddle_base_path, "rec")

    # 加载自定义训练的模型
    model = YOLO(yolo_model_path)
    # 初始化PaddleOCR并手动指定模型路径
    ocr = PaddleOCR(
        use_angle_cls=True,
        lang="en",
        show_log=False,
        det_model_dir=paddle_det_path,  # 文本检测模型路径
        rec_model_dir=paddle_rec_path,  # 英文识别模型路径
        cls_model_dir=paddle_cls_path  # 方向分类模型路径（可选）
    )

    for image_dir in os.listdir(cropped_all_images_dir):
        cropped_image_dir = os.path.join(cropped_all_images_dir, image_dir)
        cropped_label_dir = os.path.join(cropped_all_labels_dir, image_dir)
        os.makedirs(cropped_label_dir, exist_ok=True)

        # 使用模型进行预测
        results = model(cropped_image_dir, show_labels=False, show_conf=False, save=False, device='cpu')  # 对图像进行预测

        for i, result in enumerate(results):
            # 获取输入图像的文件名（不带扩展名）
            image_path = result.path
            image_full_name = os.path.splitext(os.path.basename(image_path))
            image_name = image_full_name[0]
            image_full_path = os.path.join(cropped_image_dir, f"{image_name}.jpg")

            # 读取测试图像
            test_image = cv2.imread(image_full_path)
            processed_cur_image_data = get_box_data(result, test_image, ocr)

            # 在输出文件夹中创建与图像同名的txt文件
            txt_file = os.path.join(cropped_label_dir, f"{image_name}.txt")

            # 以写入模式打开文件
            with open(txt_file, 'w') as file:
                for item in processed_cur_image_data:
                    # 将每个子列表的元素转换为字符串并用空格连接
                    line = ' '.join(map(str, item[0])) + ' ' + ' '.join(map(str, item[1])) + ' ' + image_name + ' ' + item[2]
                    file.write(line + '\n')

            print("数据已成功写入 {} 文件".format(txt_file))

        # 清除显存
        del results
        torch.cuda.empty_cache()

if __name__ == '__main__':
    repo_abs_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    station_name = 'dsa'

    predict_image(repo_abs_path, station_name)
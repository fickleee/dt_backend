import os
import re
import cv2

def detect_id(text):
    # 字符转换
    translation_table = str.maketrans({
        'Q': '0', 'O': '0', 'o': '0',
        'I': '1', 'i': '1', 'L': '1', 'l': '1',
        'Z': '2', 'z': '2',
        't': 'T', 'S': '5', 's': '5'
    })
    text = text.translate(translation_table)
    return text

def are_labels_close(label1, label2, threshold=50):
    width_threshold = threshold
    height_threshold = threshold*0.3

    """ 检查两个标签的中心点是否接近 """
    x1_1, y1_1, x2_1, y2_1, _, _, _, _, _, _ = label1
    x1_2, y1_2, x2_2, y2_2, _, _, _, _, _, _ = label2
    center_x1 = (x1_1 + x2_1) / 2
    center_y1 = (y1_1 + y2_1) / 2
    center_x2 = (x1_2 + x2_2) / 2
    center_y2 = (y1_2 + y2_2) / 2

    result = abs(center_x1 - center_x2) < width_threshold and abs(center_y1 - center_y2) < height_threshold

    return result

def are_labels_close_threshold(label1, label2, threshold=50):
    width_threshold = threshold
    height_threshold = threshold*0.3

    """ 检查两个标签的中心点是否接近 """
    x1_1, y1_1, x2_1, y2_1, _, _, _, _, _, _ = label1
    x1_2, y1_2, x2_2, y2_2, _, _, _, _, _, _ = label2
    center_x1 = (x1_1 + x2_1) / 2
    center_y1 = (y1_1 + y2_1) / 2
    center_x2 = (x1_2 + x2_2) / 2
    center_y2 = (y1_2 + y2_2) / 2

    result = abs(center_x1 - center_x2) < width_threshold and abs(center_y1 - center_y2) < height_threshold

    return result

def are_labels_same(label1, label2):
    """ 检查两个标签的文本信息是否相同 """
    _, _, _, _, related_x1, related_y1, related_x2, related_y2, _, text1 = label1
    _, _, _, _, related_x3, related_y3, related_x4, related_y4, _, text2 = label2
    # 检查 text1 是否包含数字或字母
    # if re.search(r'[a-zA-Z0-9]', text1) and text1 == text2:
    if text1 == text2:
        return True
    else:
        return False

def should_add_label(original_label, merged_labels, large_threshold, small_threshold):
    for merged_label in merged_labels:
        if are_labels_same(original_label, merged_label) and are_labels_close_threshold(original_label, merged_label, large_threshold):
            return False
        if are_labels_close(original_label, merged_label, small_threshold):
            return False
    return True

def convert_crop_to_original(label, crop_start_x, crop_start_y):
    # 提取坐标和文本信息
    x1, y1, x2, y2, path, text = label

    # 计算绝对坐标
    x1_abs = (x1 + crop_start_x)
    y1_abs = (y1 + crop_start_y)
    x2_abs = (x2 + crop_start_x)
    y2_abs = (y2 + crop_start_y)

    # 返回新的标签 (x1_abs, y1_abs, x2_abs, y2_abs, text)
    return [x1_abs, y1_abs, x2_abs, y2_abs, x1, y1, x2, y2, path, text]

def parse_yolo_labels(label_file):
    with open(label_file, 'r') as f:
        labels = []
        for line in f:
            parts = line.strip().split(' ')
            if len(parts) >= 6:  # 检查是否包含至少6个字段 (x1, y1, x2, y2, path, text)
                text = ' '.join(parts[5:])  # 将第6个及之后的字段合并成一个字段text
                text = text.replace(" ", "")  # 去掉text中所有的空格
                text = detect_id(text)  # 使用正则表达式判断并返回匹配的字符串
                labels.append([float(x) for x in parts[:4]] + [parts[4]] + [text])
            else:
                print(f"Ignoring malformed label: {line.strip()}")
    return labels

def merge_labels(label_folder, output_label_path, img_size, crop_size, overlap):
    orig_w, orig_h = img_size
    crop_w, crop_h = crop_size
    overlap_w, overlap_h = overlap

    merged_labels = []

    for label_file in sorted(os.listdir(label_folder)):
        label_path = os.path.join(label_folder, label_file)
        labels = parse_yolo_labels(label_path)

        # 获取裁剪的起始坐标
        parts = label_file.split('_')
        crop_id = int(parts[-1].split('.')[0])

        # 计算对应的裁剪起始坐标
        grid_x = crop_id % ((orig_w + overlap_w - 1) // (crop_w - overlap_w))
        grid_y = crop_id // ((orig_w + overlap_w - 1) // (crop_w - overlap_w))
        left = grid_x * (crop_w - overlap_w)
        top = grid_y * (crop_h - overlap_h)

        # 处理右侧和底部边缘
        right = min(left + crop_w, orig_w)
        bottom = min(top + crop_h, orig_h)
        if right - left < crop_w:
            left = orig_w - crop_w
        if bottom - top < crop_h:
            top = orig_h - crop_h

        for label in labels:
            original_label = convert_crop_to_original(label, left, top)

            # # 去重：检查是否已有接近且文本信息相同的标签，只有当 original_label 与 merged_labels 中的所有标签都不同时，才会将其添加到 merged_labels 中
            # if not any(are_labels_close(original_label, merged_label) and are_labels_same(original_label, merged_label) for merged_label in merged_labels):
            #     merged_labels.append(original_label)

            # 去重：检查是否已有接近且文本信息相同的标签，只有当 original_label 与 merged_labels 中的所有标签都不同时，才会将其添加到 merged_labels 中
            if should_add_label(original_label, merged_labels, large_threshold=50, small_threshold=20):
                merged_labels.append(original_label)

    # 将所有标签写入输出文件
    with open(output_label_path, 'w') as f:
        for label in merged_labels:
            f.write(' '.join(map(str, label[:8])) + ' ' + label[8] + ' ' + label[9] + '\n')


def draw_bounding_boxes(image_path, label_path, output_image_path=None):
    # 读取图片
    image = cv2.imread(image_path)
    if image is None:
        print(f"Error: Could not read image from {image_path}")
        return
    img_height, img_width, _ = image.shape

    # 读取标签文件
    with open(label_path, 'r') as f:
        labels = f.readlines()

    for label in labels:
        parts = label.strip().split()
        if len(parts) != 10:
            print(f"Skipping malformed label: {label}")
            continue

        x1, y1, x2, y2, _, _, _, _, _, text = parts
        x1, y1, x2, y2 = map(lambda x: int(float(x)), [x1, y1, x2, y2])

        # 绘制矩形框（绿色）
        cv2.rectangle(image, (x1, y1), (x2, y2), (0, 255, 0), 1)

        # 计算文本位置（放在矩形框内左上方）
        text_x = x1 + 2  # 稍微偏移，避免紧贴边框
        text_y = y1 + 12  # 放在框内顶部

        # 设置字体参数
        font = cv2.FONT_HERSHEY_SIMPLEX
        font_scale = 0.75  # 较小的字体大小
        font_thickness = 1
        text_color = (0, 0, 255)  # 红色 (BGR格式)

        # 绘制文本（红色）
        cv2.putText(
            image,
            text,
            (text_x, text_y),
            font,
            font_scale,
            text_color,
            font_thickness,
            cv2.LINE_AA
        )

    # 如果指定了输出路径，则保存图片
    if output_image_path:
        cv2.imwrite(output_image_path, image)

    return image  # 返回带标注的图像，方便后续显示或处理

def merge_image(repo_abs_path, station_name):
    cropped_all_labels_dir = os.path.join(repo_abs_path, "merge", station_name, "cropped_label")
    merged_label_dir = os.path.join(repo_abs_path, "merge", station_name, "merged_label")
    os.makedirs(merged_label_dir, exist_ok=True)

    origin_images_dir = os.path.join(repo_abs_path, "merge", station_name, "imgs")
    merged_images_dir = os.path.join(repo_abs_path, "merge", station_name, "merged_image")
    os.makedirs(merged_images_dir, exist_ok=True)

    for sub_file_dir in os.listdir(cropped_all_labels_dir):
        drawing_seg_labels_dir = os.path.join(cropped_all_labels_dir, sub_file_dir)  # 要保存的分割图片对应的标签文件的文件夹
        merged_label_file = os.path.join(merged_label_dir, f"{sub_file_dir}.txt")  # 要保存的合并后的标签文件
        origin_image_path = os.path.join(origin_images_dir, f"{sub_file_dir}.jpg") # 原图路径
        drawing_image = cv2.imread(origin_image_path)  # 对应的图片文件
        drawing_h, drawing_w, _ = drawing_image.shape
        merge_labels(
            label_folder=drawing_seg_labels_dir,
            output_label_path=merged_label_file,
            img_size=(drawing_w, drawing_h),
            crop_size=(1024, 1024),
            overlap=(512, 512)
        )
        print(f"Merged labels for {sub_file_dir} saved to {merged_label_file}")

        merged_image_path = os.path.join(merged_images_dir, f"{sub_file_dir}.jpg") # 合并标签后的图片（带有预测框 + OCR结果）

        draw_bounding_boxes(
            image_path=origin_image_path,
            label_path=merged_label_file,
            output_image_path=merged_image_path
        )
        print(f"Merged image for {sub_file_dir} saved to {merged_image_path}")

if __name__ == '__main__':
    repo_abs_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    station_name = 'tangjing'

    merge_image( repo_abs_path, station_name)
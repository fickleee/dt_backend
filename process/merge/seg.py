import cv2
import os


def crop_image(image_path, output_dir, crop_size, overlap):
    # 读取图像
    image = cv2.imread(image_path)
    if image is None:
        raise ValueError(f"无法读取图像: {image_path}")

    image_height, image_width = image.shape[:2]
    image_name = os.path.splitext(os.path.basename(image_path))[0]
    crop_width, crop_height = crop_size
    overlap_width, overlap_height = overlap

    crop_id = 0
    for top in range(0, image_height, crop_height - overlap_height):
        for left in range(0, image_width, crop_width - overlap_width):
            right = min(left + crop_width, image_width)
            bottom = min(top + crop_height, image_height)

            # 如果右边或底部区域小于裁剪尺寸，调整起始位置
            if right - left < crop_width:
                left = image_width - crop_width
                right = image_width
            if bottom - top < crop_height:
                top = image_height - crop_height
                bottom = image_height

            # 使用OpenCV裁剪图像
            crop = image[top:bottom, left:right]

            # 保存裁剪后的图像
            output_path = os.path.join(output_dir, f'{image_name}_{crop_id}.jpg')
            cv2.imwrite(output_path, crop)
            crop_id += 1


def segment_image(repo_abs_path, station_name):
    origin_images_dir = os.path.join(repo_abs_path, 'merge', station_name, 'imgs')
    cropped_all_images_dir = os.path.join(repo_abs_path, 'merge', station_name, 'image')
    os.makedirs(cropped_all_images_dir, exist_ok=True)
    
    for file in os.listdir(origin_images_dir):
        first_name, last_name = file.split(".")
        full_file_path = os.path.join(origin_images_dir, file)

        cropped_image_dir = os.path.join(cropped_all_images_dir, first_name)
        os.makedirs(cropped_image_dir, exist_ok=True)  # 创建文件夹，若已存在则不报错

        crop_image(
            image_path=full_file_path,
            output_dir=cropped_image_dir,
            crop_size=(1024, 1024),
            overlap=(512, 512)
        )
        print(f"Crop {file} done!")

if __name__ == '__main__':
    repo_abs_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    station_name = 'dsa'

    segment_image(repo_abs_path, station_name)
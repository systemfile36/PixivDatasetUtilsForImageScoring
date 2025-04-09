import os
import sys
import argparse
import shutil
from multiprocessing import Pool, cpu_count
from PIL import Image

import logger_factory

# Temp Setting
DEFAULT_TARGET_SIZE = (512, 512)  # (width, height)
ROOT_DIR = "H:/DataScripts/Temp"
VALID_EXTS = ('.jpg', '.jpeg', '.png')
RESIZE_FILTER = Image.BOX  # near to TensorFlow ResizeMethod.AREA
PADDING_COLOR = (0, 0, 0)

def find_images(root_dir) -> list[str]:
    """
    Collect all images from roo_dir (max-depth = 1)
    """
    image_files = []
    for base, dirs, files in os.walk(root_dir):
        # if depth of current base dir is more than 1, skip it
        if os.path.relpath(base, root_dir).count(os.sep) > 1:
            continue
        for file in files:
            # check extension 
            if file.lower().endswith(VALID_EXTS):
                image_files.append(os.path.join(base, file))
    return image_files

def resize_with_aspect_ratio(img: Image, target_size=(512, 512), bg_color=(0, 0, 0)):
    """
    Resize with preserve aspect ratio
    """
    original_width, original_height = img.size
    target_width, target_height = target_size

    # preserve aspect ratio
    ratio = min(target_width / original_width, target_height / original_height)
    new_width = int(original_width * ratio)
    new_height = int(original_height * ratio)

    # excute resize
    resized_img = img.resize((new_width, new_height), RESIZE_FILTER)

    # generate padding image
    new_img = Image.new("RGB", target_size, bg_color)
    offset_x = (target_width - new_width) // 2
    offset_y = (target_height - new_height) // 2
    new_img.paste(resized_img, (offset_x, offset_y))

    return new_img

def process_image(path, target_size: tuple[int, int]):
    try:
        dir_name, file_name = os.path.split(path)
        name, _ = os.path.splitext(file_name)
        resized_temp_path = os.path.join(dir_name, f"{name}_resized.png")
        final_path = os.path.join(dir_name, f"{name}.png")

        with Image.open(path) as img:
            img = img.convert("RGB")
            resized = resize_with_aspect_ratio(img, target_size, PADDING_COLOR)
            resized.save(resized_temp_path, format="PNG")

        os.remove(path)  # remove source file
        os.replace(resized_temp_path, final_path)  # rename temp to source file's name
        return f"[OK] {file_name}"
    except Exception as e:
        return f"[ERROR] {file_name}: {e}"


if __name__ == "__main__":

    logger = logger_factory.get_default_logger(__name__)

    parser = argparse.ArgumentParser(description="Resize Image with preserve aspect ratio")
    parser.add_argument(
        "--size",
        nargs=2,
        type=int,
        metavar=('WIDTH', 'HEIGHT'),
        required=True,
        help="Target size of resize. ex) --size 512 512."
    )

    parser.add_argument(
        "--root",
        type=str,
        help="Target Root directory to resize."
    )
    args = parser.parse_args()

    target_size = tuple(args.size)
    root_dir = args.root

    image_files = find_images(root_dir)
    logger.info(f"Total Image count : {len(image_files)}")

    # generate list[tuple] to pass arguments to task
    tasks = [(path, target_size) for path in image_files]

    with Pool(processes=cpu_count()) as pool:
        for result in pool.starmap(process_image, tasks):
            logger.debug(result)


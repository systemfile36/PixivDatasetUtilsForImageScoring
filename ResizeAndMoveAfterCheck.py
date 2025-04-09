import os
import shutil
import re
import argparse
from PIL import Image
from multiprocessing import Pool, cpu_count

from logger_factory import get_default_logger
  
from ResizeImageWithAspectRatio import resize_with_aspect_ratio

TARGET_SIZE = (512, 512)
RESIZE_FILTER = Image.BOX
PADDING_COLOR = (0, 0, 0)
VALID_EXTS = ('.jpg', '.jpeg', '.png')

pattern = re.compile(r'^(?P<id>\d{6,10})_p\d+\.(jpg|jpeg|png)$', re.IGNORECASE)

def process_single_file(full_path):
    """
    Check file exists by prefix(file_name[:2]).
    If file already exists, remove source, otherwise, move to prefix dir and remove source
    """

    logger = get_default_logger(__name__)
    
    if not os.path.isfile(full_path):
        logger.debug(f"[SKIP] There is no file: {full_path}")
        return

    # split directory path and file name
    dir_path, file_name = os.path.split(full_path)
    name, ext = os.path.splitext(file_name)
    
    json_path = full_path + '.json'
    has_json = os.path.isfile(json_path)

    # check filename 
    match = pattern.match(file_name if not file_name.endswith('.json') else name)
    if not match:
        logger.debug(f"[SKIP] Not match to Regex: {file_name}")
        return

    # Get prefix and make directory which named prefix
    id_part = match.group('id')
    prefix = id_part[:2]
    target_dir = os.path.join(dir_path, prefix)
    os.makedirs(target_dir, exist_ok=True)

    # check file exists
    target_file_path = os.path.join(target_dir, file_name)
    if os.path.exists(target_file_path):
        logger.debug(f"[DUPLICATE] {file_name} → Remove")
        os.remove(full_path)
        if has_json:
            os.remove(json_path)
        return

    # Resize + Move
    try:
        if ext.lower() in VALID_EXTS:
            with Image.open(full_path) as img:
                img = img.convert("RGB")
                resized = resize_with_aspect_ratio(img, TARGET_SIZE, PADDING_COLOR)

                # Resize and save to target directory
                save_path = os.path.join(target_dir, name + ".png")
                resized.save(save_path, format="PNG")

        # move json file too
        if has_json:
            target_json_path = os.path.join(target_dir, os.path.basename(json_path))
            shutil.move(json_path, target_json_path)

        # remove source file
        os.remove(full_path)

        logger.debug(f"[MOVED] {file_name} → {target_dir}")
    except Exception as e:
        logger.error(f"[ERROR] {file_name}: {e}")

def collect_files_in_directory(directory_path):
    """
    Collect file list 
    """
    files = []
    with os.scandir(directory_path) as entries:
        for entry in entries:
            if entry.is_file():
                files.append(os.path.join(directory_path, entry.name))
    return files

def run_multiprocessing(file_paths, workers=None):
    """
    using multiprocessing
    """
    with Pool(processes=workers or cpu_count()) as pool:
        pool.map(process_single_file, file_paths)

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Check the file is exists and resize it and move to subdirectory with prefix"
    )

    parser.add_argument(
        "path",
        type=str,
        help="directory path or full-path of file to process."
    )

    args = parser.parse_args()

    # If path is directory, excute recursive in directory (max-depth=1)
    if(os.path.isdir(args.path)):
        files = collect_files_in_directory(args.path)
        run_multiprocessing(files)
    else:
        process_single_file(args.path)
        

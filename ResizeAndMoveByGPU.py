import os
import re
import shutil
from PIL import Image
import argparse
from tqdm import tqdm

# Supress warning. 
# Ignore all WARNING. only logging ERROR
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'

import tensorflow as tf
import numpy as np

from logger_factory import get_file_handler, get_default_stream_handler, get_custom_handlers_logger

TARGET_SIZE = (512, 512)
PADDING_COLOR = (0, 0, 0)
VALID_EXTS = ('.jpg', '.jpeg', '.png')

pattern = re.compile(r'^(?P<id>\d{6,10})_p\d+\.(jpg|jpeg|png)$', re.IGNORECASE)

logger_handlers = [
    get_file_handler(log_prefix=os.path.basename(__file__)),
    get_default_stream_handler()
]
logger = get_custom_handlers_logger(__file__, logger_handlers)

def collect_image_files(root_dir: str) -> list[str]:
    """
    collect only image file

    max-depth is 1 (only collect not distributed image file)
    """
    files = []
    with os.scandir(root_dir) as entries:
        for entry in entries:
            if entry.is_file() and entry.name.lower().endswith(VALID_EXTS):
                files.append(os.path.join(root_dir, entry.name))
    return files

def preprocess_and_resize(image_path):
    """
    Resize using tensorflow
    """
    # read image
    raw = tf.io.read_file(image_path)
    img = tf.io.decode_image(raw, channels=3, expand_animations=False)

    # convert to float (normalize)
    img = tf.image.convert_image_dtype(img, tf.float32)

    size = (TARGET_SIZE[1], TARGET_SIZE[0])

    # resize with preserving aspect ratio
    resized = tf.image.resize_with_pad(img, size[0], size[1], method=tf.image.ResizeMethod.AREA)

    # convert back to uint8 
    resized = tf.image.convert_image_dtype(resized, tf.uint8)
    return resized, image_path

def save_resized_image(resized_tensor, image_path_tensor):
    """
    Save resized tensor by `.png` and move it to id-prefix sub directory with metadata (.json)
    this method remove source file
    """
    image_path = image_path_tensor.numpy().decode("utf-8")
    dir_path, file_name = os.path.split(image_path)
    name, ext = os.path.splitext(file_name)

    # check ragex
    match = pattern.match(file_name)
    if not match:
        logger.debug(f"[SKIP] Not match: {file_name}")
        return

    # get prefix and make sub directory
    id_part = match.group('id')
    prefix = id_part[:2]
    target_dir = os.path.join(dir_path, prefix)
    os.makedirs(target_dir, exist_ok=True)

    # target path
    final_image_path = os.path.join(target_dir, name + ".png")
    if os.path.exists(final_image_path):
        logger.debug(f"[DUPLICATE] {file_name} already exists. Deleting original.")
        os.remove(image_path)
        json_path = image_path + ".json"
        if os.path.exists(json_path):
            os.remove(json_path)
        return

    # convert tensor to numpy array
    img_array = resized_tensor.numpy()

    # convert numpy array to PIL Image instance
    img = tf.keras.utils.array_to_img(img_array)
    img.save(final_image_path, format="PNG")

    # move metadata file 
    json_path = image_path + ".json"
    if os.path.exists(json_path):
        shutil.move(json_path, os.path.join(target_dir, os.path.basename(json_path)))

    # remove source
    os.remove(image_path)
    logger.debug(f"[MOVED] {file_name} → {target_dir}")

def run_pipeline(image_paths, batch_size=32):
    """
    TensorFlow tf.data.Dataset pipeline
    """
    ds = tf.data.Dataset.from_tensor_slices(image_paths)
    # parallel work by map
    ds = ds.map(preprocess_and_resize, num_parallel_calls=tf.data.AUTOTUNE)
    #use minibatch
    ds = ds.batch(batch_size)
    ds = ds.prefetch(buffer_size=tf.data.AUTOTUNE)

    total = len(image_paths)

    with tqdm(total=total, desc="Processing images", unit="image") as progress:
        for resized_batch, path_batch in ds:
            for i in range(resized_batch.shape[0]):
                save_resized_image(resized_batch[i], path_batch[i])
                # update progress bar
                progress.update(1)

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
        logger.info("Searching image files...")
        files = collect_image_files(args.path)
        logger.info(f"Image count : {len(files)}. Start GPU processing...")
        run_pipeline(files)
    else:
        save_resized_image(preprocess_and_resize(args.path))
        


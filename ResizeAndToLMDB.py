import os
import re
import json
import sqlite3
import argparse
import lmdb
import tarfile
import tensorflow as tf
import numpy as np
from tqdm import tqdm
from pathlib import Path
from io import BytesIO
from datetime import datetime
from logger_factory import get_file_handler, get_default_stream_handler, get_custom_handlers_logger

# TensorFlow warning suppression
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'

# Constants for image extensions
VALID_EXTS = ('.jpg', '.jpeg', '.png')
IMAGE_EXTENSIONS = VALID_EXTS
pattern = re.compile(r'^(?P<id>\d{6,10})_p\d+\.(jpg|jpeg|png)$', re.IGNORECASE)  # Regex for image filename

# Logger setup
logger_handlers = [
    get_file_handler(log_prefix=os.path.basename(__file__)),
    get_default_stream_handler()
]
logger = get_custom_handlers_logger(__file__, logger_handlers)

def init_db(path: str):
    """CreateTable"""
    conn = sqlite3.connect(path)
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS illusts (
        filename TEXT PRIMARY KEY,
        id INTEGER,
        title TEXT,
        type TEXT,
        restrict INTEGER,
        user_name TEXT,
        user_account TEXT,
        tags TEXT,
        create_date TIMESTAMP,
        page_count INTEGER,
        width INTEGER,
        height INTEGER,
        sanity_level INTEGER,
        x_restrict INTEGER,
        total_view INTEGER,
        total_bookmarks INTEGER,
        is_bookmarked BOOLEAN,
        visible BOOLEAN,
        is_muted BOOLEAN,
        illust_ai_type INTEGER,
        illust_book_style INTEGER,
        num INTEGER,
        date TIMESTAMP,
        rating TEXT,
        suffix TEXT,
        category TEXT,
        subcategory TEXT,
        url TEXT,
        date_url TEXT,
        extension TEXT
    );
    """)
    conn.commit()
    conn.close()

# Collect image files from directory
def collect_image_files(root_dir: str) -> list[str]:
    """
    collect only image file

    max-depth is 1
    """
    files = []
    with os.scandir(root_dir) as entries:
        for entry in entries:
            if entry.is_file() and entry.name.lower().endswith(VALID_EXTS):
                files.append(os.path.join(root_dir, entry.name))
    return files

def collect_all_image_files(root_path: str) -> list[str] | None:
    """
    Collect all image files in `root_path`

    no max-depth limit
    """
    
    path = Path(root_path)

    try:
        image_files = list(path.rglob("*.png") + list(path.rglob("*.jpg")))
        return image_files
    except Exception as e:
        logger.error(f"Failed to collect image files: {e}")
        return None

# Read and resize image to target size using TensorFlow
def preprocess_and_resize(image_path, target_size):
    """
    Resize using tensorflow
    """
    # read image
    raw = tf.io.read_file(image_path)
    img = tf.io.decode_image(raw, channels=3, expand_animations=False)

    # convert to float (normalize)
    img = tf.image.convert_image_dtype(img, tf.float32)
    size = (target_size[1], target_size[0])  # Note: height, width

    # resize with preserving aspect ratio
    resized = tf.image.resize_with_pad(img, size[0], size[1], method=tf.image.ResizeMethod.AREA)
    
    # convert back to uint8 
    resized = tf.image.convert_image_dtype(resized, tf.uint8)
    return resized, image_path

# Encode image tensor to PNG bytes
def encode_image_tensor(tensor):
    """
    Encode image tensor to PNG bytes (numpy)

    Dtype of tensor should be `uint8`
    """
    img = tf.io.encode_png(tensor)
    return img.numpy()

# Store encoded image bytes into LMDB
def store_to_lmdb(txn, key: str, image_bytes: bytes):
    """
    Store encoded image bytes into LMDB
    """
    try:
        txn.put(key.encode(), image_bytes)
        logger.debug(f"Stored to LMDB with key: {key}")
    except Exception as e:
        logger.error(f"Failed to write {key} to LMDB: {e}")

# Parse associated JSON metadata file
def parse_json(json_path: str):

    try:
        with open(json_path, "r", encoding="utf-8") as f:
            metadata = json.load(f)
    except Exception as e:
        logger.error(f"[ERROR] Failed to parse JSON: {json_path} - {e}")
        return None

    def parse_datetime(s):
        try:
            return datetime.fromisoformat(s.replace("Z", "+00:00")).isoformat(sep=' ', timespec='seconds') if s else None
        except Exception as e:
            logger.warning(f"Invalid date format {s} - {e}")
            return None

    try:
        return {
            "id": metadata.get("id"),
            "title": metadata.get("title"),
            "type": metadata.get("type"),
            "restrict": metadata.get("restrict"),
            "user_name": metadata.get("user", {}).get("name"),
            "user_account": metadata.get("user", {}).get("account"),
            "tags": ",".join(metadata.get("tags", [])),
            "create_date": parse_datetime(metadata.get("create_date")),
            "page_count": metadata.get("page_count"),
            "width": metadata.get("width"),
            "height": metadata.get("height"),
            "sanity_level": metadata.get("sanity_level"),
            "x_restrict": metadata.get("x_restrict"),
            "total_view": metadata.get("total_view"),
            "total_bookmarks": metadata.get("total_bookmarks"),
            "is_bookmarked": metadata.get("is_bookmarked"),
            "visible": metadata.get("visible"),
            "is_muted": metadata.get("is_muted"),
            "illust_ai_type": metadata.get("illust_ai_type"),
            "illust_book_style": metadata.get("illust_book_style"),
            "num": metadata.get("num"),
            "date": parse_datetime(metadata.get("date")),
            "rating": metadata.get("rating"),
            "suffix": metadata.get("suffix"),
            "category": metadata.get("category"),
            "subcategory": metadata.get("subcategory"),
            "url": metadata.get("url"),
            "date_url": metadata.get("date_url"),
            "filename": metadata.get("filename"),
            "extension": metadata.get("extension"),
        }
    except Exception as e:
        logger.error(f"[ERROR] Failed to extract field: {json_path} - {e}")
        return None

# Store metadata dictionary to SQLite DB
def store_metadata_to_db(conn: sqlite3.Connection, record: dict):
    try:
        cursor = conn.cursor()
        placeholders = ", ".join(["?"] * len(record))
        columns = ", ".join(record.keys())
        sql = f"INSERT OR REPLACE INTO illusts ({columns}) VALUES ({placeholders})"
        cursor.execute(sql, list(record.values()))
        conn.commit()
        logger.debug(f"Inserted metadata into DB for ID: {record.get('id')}")
    except Exception as e:
        logger.error(f"Failed to insert metadata into DB: {e}")

# Main processing logic for directory of images
def process_and_store(image_paths, lmdb_path: str, target_size, db_path: str, batch_size=32):
    env = lmdb.open(lmdb_path, map_size=1 << 39)  # Large LMDB map size
    ds = tf.data.Dataset.from_tensor_slices(image_paths)
    ds = ds.map(lambda path: preprocess_and_resize(path, target_size), num_parallel_calls=tf.data.AUTOTUNE)
    ds = ds.batch(batch_size)
    ds = ds.prefetch(buffer_size=tf.data.AUTOTUNE)

    conn = sqlite3.connect(db_path)

    total = len(image_paths)
    with tqdm(total=total, desc="Processing images", unit="image") as progress:
        for resized_batch, path_batch in ds:

            # Begin LMDB transaction
            # Transaction per batch
            txn = env.begin(write=True)

            for i in range(resized_batch.shape[0]):
                try:
                    image_path = path_batch[i].numpy().decode("utf-8")
                    file_name = os.path.basename(image_path)
                    match = pattern.match(file_name)
                    if not match:
                        logger.debug(f"[SKIP] Not matched pattern: {file_name}")
                        continue
                    
                    # Encode image tensor -> numpy bytes
                    image_bytes = encode_image_tensor(resized_batch[i])
                    store_to_lmdb(txn, file_name, image_bytes)

                    json_path = image_path + ".json"
                    if os.path.exists(json_path):
                        record = parse_json(json_path)
                        if record:
                            store_metadata_to_db(conn, record)
                        # os.remove(json_path)  # Remove JSON after processing

                    os.remove(image_path)  # Remove image after processing
                    logger.debug(f"[REMOVED] {file_name}")
                except Exception as e:
                    logger.warning(f"Failed to process {image_path}: {e}")
                progress.update(1)
            
            try:
                txn.commit()
            except Exception as e:
                logger.error(f"Failed to commit batch")
                txn.abort()

    conn.close()
    env.close()
    logger.info("LMDB processing completed.")

def archive_json_files(root_path: Path):
    """
    Archives all .json files in the dataset directory into a single .tar file.
    This preserves metadata while reducing the file count.
    """
    try:
        json_files = list(root_path.rglob("*.json"))
        if not json_files:
            logger.info("No JSON files found to archive.")
            return

        archive_path = root_path / "metadata_jsons.tar"
        with tarfile.open(archive_path, "w") as tar:
            for json_file in json_files:
                try:
                    tar.add(json_file, arcname=json_file.relative_to(root_path))
                    logger.debug(f"Added {json_file} to archive")
                except Exception as e:
                    logger.warning(f"Failed to add {json_file} to archive: {e}")

        logger.info(f"Archived {len(json_files)} JSON files into {archive_path}")
    except Exception as e:
        logger.error(f"Failed to archive JSON files: {e}")

# Main CLI logic
def main():
    parser = argparse.ArgumentParser(description="Resize images and store into LMDB")
    parser.add_argument("--root-path", type=str, required=True, help="abolute path to image directory")
    parser.add_argument("--lmdb-path", type=str, required=True, help="abolute or relative path to LMDB file")
    parser.add_argument("--db-path", type=str, required=True, help="abolute or relative path to SQLite database")
    parser.add_argument("--batch_size", type=int, default=32, help="Batch size for TensorFlow pipeline")
    parser.add_argument("--target_size", type=int, nargs=2, metavar=("WIDTH", "HEIGHT"), default=(512, 512), help="Target size for resizing images")
    args = parser.parse_args()

    if os.path.isdir(args.root_path):

        logger.info(f"Initialize DB {args.db_path}")
        init_db(args.db_path)
        logger.info("Collecting image files...")
        files = collect_all_image_files(args.root_path)
        if files is None:
            return
        logger.info(f"Found {len(files)} image files. Starting processing...")
        process_and_store(files, args.lmdb_path, target_size=args.target_size, db_path=args.db_path, batch_size=args.batch_size)
        archive_json_files(Path(args.root_path))
    else:
        logger.error(f"{args.pat}")

if __name__ == "__main__":
    main()
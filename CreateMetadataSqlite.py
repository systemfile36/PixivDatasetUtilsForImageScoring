import os
import json
import sqlite3
from multiprocessing import Pool, cpu_count
from functools import partial
import argparse
import logger_factory
from datetime import datetime

from tqdm import tqdm

IMAGE_EXTENSIONS = [".png", ".jpg", ".jpeg"]

logger_handlers = [
    logger_factory.get_file_handler(log_prefix=os.path.basename(__file__)),
    logger_factory.get_default_stream_handler()
]
logger = logger_factory.get_custom_handlers_logger(__file__, logger_handlers)

def init_db(path: str):
    """CreateTable"""
    conn = sqlite3.connect(path)
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS illusts (
        id INTEGER PRIMARY KEY,
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
        filename TEXT,
        extension TEXT
    );
    """)
    conn.commit()
    conn.close()

def find_json_files(root_dir):
    """Find all json files in root_dir recursively"""
    return [
        os.path.join(dirpath, filename)
        for dirpath, _, filenames in os.walk(root_dir)
        for filename in filenames
        if filename.lower().endswith(".json")
    ]

def parse_json(json_path):
    """Parsing JSON and extract metadata"""
    # json_path e.g. /data/29/29182021_p0.jpg.json
    base_name = os.path.splitext(os.path.basename(json_path))[0]  # e.g. 29182021_p0.jpg
    base_name = os.path.splitext(base_name)[0] # e.g. 29182021_p0
    subdir = os.path.dirname(json_path) # e.g. 29

    #logger.debug(f"Parsing {base_name}...")

    # Check existence of image
    found_image = None
    for ext in IMAGE_EXTENSIONS:
        if os.path.isfile(os.path.join(subdir, base_name + ext)):
            found_image = True
            break
    if not found_image:
        return None  # If image does not exists, return None
    
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            metadata = json.load(f)
    except Exception as e:
        logger.error(f"[ERROR] Failed to parsing JSON: {json_path} - {e}")
        return None

    def parse_datetime(s):
        try:
            # Convert to ISO 8601 format (YYYY-MM-DD HH:MM:SS)
            return datetime.fromisoformat(s.replace("Z", "+00:00")).isoformat(sep=' ', timespec='seconds') if s else None
        except:
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

def insert_batch(records, db_path):
    """Insert records batch"""

    if not records:
        return
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        conn.execute("BEGIN TRANSACTION;")
        for record in records:
            placeholders = ", ".join(["?"] * len(record))
            columns = ", ".join(record.keys())
            sql = f"INSERT OR REPLACE INTO illusts ({columns}) VALUES ({placeholders})"
            cursor.execute(sql, list(record.values()))
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"[ERROR] Fail in Transaction: {e}")
    finally:
        conn.close()

def main():
    parser = argparse.ArgumentParser(
        description="Create metadata database from .json file. Table scheme is match to 'Pixiv API'"
    )

    parser.add_argument(
        "root_path",
        type=str,
        help="full path of directory path"
    )

    parser.add_argument(
        "db_path",
        type=str,
        help="full path of database path (.sqlite)"
    )

    args = parser.parse_args()

    logger.debug(f"Input: root_path={args.root_path}, db_path={args.db_path}")

    if not os.path.isdir(args.root_path):
        logger.error(f"{args.root_path} is not directory.")
        return

    logger.info(f"Initialize database: {args.db_path}")
    init_db(args.db_path)

    logger.info(f"Search JSON files in {args.root_path}")
    json_files = find_json_files(args.root_path)

    logger.info(f"Start multiprocessing...")

    with Pool(processes=cpu_count()) as pool:
        results = pool.map(parse_json, json_files)

    # filtering none
    valid_records = [result for result in tqdm(results, total=len(json_files)) if result is not None]
    logger.info(f"valid records: {len(valid_records)}")

    logger.info("Insert into db by transaction...")
    insert_batch(valid_records, args.db_path)

    logger.info("Complete!")

if __name__ == "__main__":
    main()



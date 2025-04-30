import json
import sqlite3
import requests
import argparse
import time
import os
import logger_factory

MAX_RETRIES = 3
RETRY_DELAY = 5

logger_handlers = [
    logger_factory.get_file_handler(log_prefix=os.path.basename(__file__)),
    logger_factory.get_default_stream_handler()
]
logger = logger_factory.get_custom_handlers_logger(__file__, logger_handlers)

def fetch_other_names(tag_name: str, username: str, api_key: str):
    """
    fetch aliases from Danbooru API
    """
    url = f"https://danbooru.donmai.us/wiki_pages.json?search[title]={tag_name}&login={username}&api_key={api_key}"

    # Repeat until attempt reach MAX_RETRIES
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # send request and raise HTTPError if has occurred
            response = requests.get(url, timeout=15)
            response.raise_for_status()

            # get response body by json
            data = response.json()
            if data and isinstance(data, list):
                # join to one string
                return ",".join(data[0].get("other_names", []))
            break 
        except Exception as e:
            logger.warning(f"[WARN] Attempt {attempt} failed for '{tag_name}': {e}")
            if attempt < MAX_RETRIES:
                logger.info(f"[INFO] Retrying in {RETRY_DELAY} seconds...")
                time.sleep(RETRY_DELAY)
            else:
                logger.error(f"[ERROR] All retries failed for '{tag_name}'")

    return "(none)"

def main(tag_file_path, output_db_path, username: str, api_key: str):
    if not os.path.exists(tag_file_path):
        logger.error(f"[ERROR] Tag file not found: {tag_file_path}")
        return

    with open(tag_file_path, "r", encoding="utf-8") as f:
        tags_data = json.load(f)

    conn = sqlite3.connect(output_db_path)
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS character_tags (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        danbooru_post_count INTEGER, 
        created_at TIMESTAMP,
        other_names TEXT NOT NULL
    )
    """)

    cursor.execute("BEGIN TRANSACTION;")

    for tag in tags_data:
        name = tag.get("name")
        post_count = tag.get("post_count", 0)
        created_at = tag.get("created_at")

        other_names = fetch_other_names(name, username, api_key)

        cursor.execute("""
        INSERT OR REPLACE INTO character_tags (name, danbooru_post_count, created_at, other_names)
        VALUES (?, ?, ?, ?)
        """, (name, post_count, created_at, other_names))

        logger.info(f"[INFO] Inserted: {name}")
        time.sleep(0.5)  # interval for avoid rate limit 

    conn.commit()
    conn.close()
    logger.info(f"[DONE] Character Tags dataase saved: {output_db_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create database of Danbooru character Tags with aliases(other_names from Danbooru wiki_pages)")
    
    parser.add_argument(
        "--tag-file", 
        type=str,
        required=True, 
        help="JSON file contain Tags. It's same to the output of Danbooru tags.json API")
    
    parser.add_argument(
        "--output-db",
        type=str,
        required=True, 
        help="absolute or relative path of output sqlite file")
    
    parser.add_argument(
        "--username",
        type=str,
        required=True,
        help="username of Danbooru Account"
    )

    parser.add_argument(
        "--api-key",
        type=str,
        required=True,
        help="api_key of Danbooru Account"
    )

    args = parser.parse_args()

    main(args.tag_file, args.output_db, args.username, args.api_key)

import sqlite3
import json
import re
import argparse
import os
import logger_factory

logger_handlers = [
    logger_factory.get_file_handler(log_prefix=os.path.basename(__file__)),
    logger_factory.get_default_stream_handler()
]
logger = logger_factory.get_custom_handlers_logger(__file__, logger_handlers)

# Regex to check Japanese (hiragana, katakana, kanji)
JPN_PATTERN = re.compile(r'[\u3040-\u30FF\u4E00-\u9FFF]')

TABLE_NAME = "character_tags"
COLUMN_NAME = "other_names"

def is_japanese(text: str):
    """
    Check 'text' has jpapanese character
    """
    return bool(JPN_PATTERN.search(text))

def main(alias_db_path: str, target_path: str):

    logger.info(f"Connect to db {alias_db_path}...")
    conn = sqlite3.connect(alias_db_path)
    cursor = conn.cursor()

    results = []

    # Get aliases from db
    cursor.execute(f"SELECT {COLUMN_NAME} FROM {TABLE_NAME}")
    rows = cursor.fetchall()
    
    for other_names in rows:

        # split and filter aliases 
        alias_list = [n.strip() for n in other_names[0].split(",") if is_japanese(n.strip())]

        if alias_list:
            #Join with ' OR ' for query
            tags = " OR ".join(alias_list)
            results.append({"tag_name": tags})

    logger.info(f"Parsing complete. save to {target_path}")
    
    with open(target_path, "w", encoding="utf-8") as file:
        json.dump(results, file, ensure_ascii=False, indent=2)

    logger.info(f"Script complete!")

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Convert Danbooru aliases to Japanese tag query list like [{\"tag_name\": \"初音ミク OR ミク\"}, ...")
    
    parser.add_argument(
        "--db-path", 
        type=str,
        required=True, 
        help=f"absolute or relative path of database that has \"{TABLE_NAME}\" table with \"{COLUMN_NAME}\" column")
    
    parser.add_argument(
        "--target-path",
        type=str,
        required=True, 
        help="absolute or relative path of output json file")

    args = parser.parse_args()

    main(args.db_path, args.target_path)
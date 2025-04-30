import sqlite3
from collections import Counter
import argparse
import os

import logger_factory

logger_handlers = [
    logger_factory.get_file_handler(log_prefix=os.path.basename(__file__)),
    logger_factory.get_default_stream_handler()
]
logger = logger_factory.get_custom_handlers_logger(__file__, logger_handlers)


def count_tags_from_pixiv_metadata(db_path: str, target_path: str):

    logger.info("Load database...")
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute("SELECT tags FROM illusts")
    except Exception as e:
        logger.error(f"Invalid database scheme : {e}")
        return

    tag_counter = Counter()

    for row in cursor:
        tag_string: str = row[0]

        # split tags to list
        tags = tag_string.strip().split(",")

        # update to counter
        tag_counter.update(tags)

    conn.close()

    logger.info(f"Counting complete! Save to {target_path}...")

    output_conn = sqlite3.connect(target_path)

    output_cursor = output_conn.cursor()

    output_cursor.execute("""
        CREATE TABLE IF NOT EXISTS tag_counts (
            tag TEXT PRIMARY KEY,
            count INTEGER NOT NULL
        );
""")
    
    try:
        output_cursor.execute("BEGIN TRANSACTION;")
        output_cursor.executemany(
            "INSERT INTO tag_counts(tag, count) VALUES (?, ?)",
            tag_counter.items()
        )
        output_conn.commit()
    except Exception as e:
        logger.error("Save fail! rollback now...")
        output_conn.rollback()

if __name__ == "__main__":
        
    parser = argparse.ArgumentParser(
        description="Count tags from pixiv metadata base and extract to json file"
    )

    parser.add_argument(
        "db_path",
        type=str,
        help="absolute or relative path of database file"
    )

    parser.add_argument(
        "target_path",
        type=str,
        help="abolute or relative path of json file to save"
    )

    args = parser.parse_args()

    count_tags_from_pixiv_metadata(args.db_path, args.target_path)
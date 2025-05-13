import sqlite3
import os 
import argparse
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor

import logger_factory

logger_handlers = [
    logger_factory.get_file_handler(log_prefix=os.path.basename(__file__)),
    logger_factory.get_default_stream_handler()
]
logger = logger_factory.get_custom_handlers_logger(__file__, logger_handlers)

DEFAULT_META_TABLE_NAME = "illusts"
DEFAULT_META_PK_COLUMN = "filename"
DEFAULT_META_PK_TYPE = "TEXT"
DEFAULT_META_TAGS_COLUMN = "tags"

DEFAULT_ALIASE_TABLE_NAME = "character_tags"

COLUMN_DEFAULT_VALUE = "オリジナル"

def match_aliases_for_record(record: tuple[any, str], aliases: list[tuple[str, str]]) -> tuple[any, str]:
    """
    For Multiprocessing.

    'aliases' is list of (name, other_names) tuple
    
    'record' is (pk, tag_string) tuple
    """

    # Extract tuple
    pk, tag_string = record
    tag_character_values = []

    # Split tag_string to list. for check full-match to aliases
    tag_string = tag_string.split(",")

    # Check all aliase from aliases
    for name, other_names in aliases:
        # split 'other_names' and convert to list 
        aliase_list = [n.strip() for n in other_names.split(",")]
            
        # Check current row's tag_string contain current aliase
        for aliase in aliase_list:
            if aliase in tag_string:
                tag_character_values.append(name)
                break

    # If there is no match aliase in tag_string, 
    # Set to default value meaning unknown character
    if len(tag_character_values) == 0:
        tag_character_values.append(COLUMN_DEFAULT_VALUE)

    return (pk, ",".join(tag_character_values))

def wrapper_for_process_pool(record_with_aliases: tuple[tuple[any, str], list[tuple[str, str]]]) -> tuple[any, str]:
    """
    Wrapper for 'match_aliases_for_record' to use ProcessPool.executor.map
    """
    
    return match_aliases_for_record(record_with_aliases[0], record_with_aliases[1])

def is_column_exists(conn: sqlite3.Connection, table_name: str, column_name: str) -> bool:
    """
    Return True if 'table_name' has column named 'column_name', otherwise, False
    """
    cursor = conn.execute(f"PRAGMA table_info('{table_name}');")
    return any([row[1] == column_name for row in cursor.fetchall()])

def add_tag_character_column(base_db_path: str, aliase_db_path: str,
                             metadata_table_name: str=DEFAULT_META_TABLE_NAME, 
                             aliase_table_name: str=DEFAULT_ALIASE_TABLE_NAME):
    """
    Add 'tag_character' column to Pixiv metadata database
    and update it to Danbooru character tag name.
    aliase_table_name must contain 'name TEXT' and 'other_names TEXT' column.
    value of 'other_names TEXT' column must be comma-seperated string list.

    To create aliase_table, use 'CreateCharacterTagsSqlite.py' in same repo 
    """

    logger.info(f"Connect to metadata database: {base_db_path}")
    base_conn = sqlite3.connect(base_db_path)
    base_cursor = base_conn.cursor()

    logger.info(f"Connect to aliase database: {aliase_db_path}")
    aliase_conn = sqlite3.connect(aliase_db_path)
    aliase_cursor = aliase_conn.cursor()

    logger.info(f"Get (name, other_names) from {aliase_table_name} table")

    # Get name and other_names and fetchall
    # filter 'other_names'
    aliase_cursor.execute(f"""
    SELECT name, other_names FROM {aliase_table_name}
    WHERE other_names != '' AND other_names != '(none)';
                          """)
    aliases = aliase_cursor.fetchall()

    aliase_conn.close()

    # Check 'tag_character' already exists.
    # SQLite does not support using 'IF NOT EXISTS' in 'ALTER TABLE'
    if not is_column_exists(base_conn, DEFAULT_META_TABLE_NAME, 'tag_character'):
        # Add column to metadata database
        base_cursor.execute(f"""
        ALTER TABLE {metadata_table_name} 
        ADD COLUMN tag_character TEXT DEFAULT '{COLUMN_DEFAULT_VALUE}'
                            """)
    
    # Create temporary table to save record that will be updated
    base_cursor.execute(f"""
    CREATE TEMP TABLE to_update (
        {DEFAULT_META_PK_COLUMN} {DEFAULT_META_PK_TYPE} PRIMARY KEY,
        tag_character TEXT    
    );
                        """)

    logger.info(f"Get {DEFAULT_META_PK_COLUMN} and {DEFAULT_META_TAGS_COLUMN} from {metadata_table_name} table")
    
    # Get PK and tags string only tag_character not applied
    base_cursor.execute(f"""
    SELECT {DEFAULT_META_PK_COLUMN}, {DEFAULT_META_TAGS_COLUMN}
    FROM {metadata_table_name}
    WHERE tag_character = '{COLUMN_DEFAULT_VALUE}' OR tag_character = '';
    """)

    # Fetch all records immediately for multiprocessing
    records = base_cursor.fetchall()

    logger.info(f"Check all aliases from {aliase_db_path}")

    # Packing to (record, aliases) for multiprocessing
    args_iterable = [(record, aliases) for record in records]

    logger.info(f"Processing {len(records)} records parellel...")

    # Process records parallel. 
    with ProcessPoolExecutor() as executor:
        to_update_records = list(tqdm(executor.map(
            wrapper_for_process_pool, 
            args_iterable,
            chunksize=50
        ), total=len(args_iterable), desc="Processing all records", 
        unit="record", leave=True))

    
    base_cursor.executemany(f"""
    INSERT OR REPLACE INTO to_update({DEFAULT_META_PK_COLUMN}, tag_character)
    VALUES (?, ?)
    """, to_update_records)

    logger.info(f"Update {metadata_table_name}...")

    base_cursor.execute(f"""
    UPDATE {metadata_table_name}
    SET tag_character = (
        SELECT tag_character FROM to_update
        WHERE to_update.{DEFAULT_META_PK_COLUMN} = {metadata_table_name}.{DEFAULT_META_PK_COLUMN}
    )
    WHERE {DEFAULT_META_PK_COLUMN} IN (SELECT {DEFAULT_META_PK_COLUMN} FROM to_update);
    """)

    base_conn.commit()
    base_conn.close()

if __name__ == "__main__":
        
    parser = argparse.ArgumentParser(
        description=f"Add 'tag_character' column to Pixiv metadata table. \
        'tag_character' column has Danbooru character tags from {DEFAULT_ALIASE_TABLE_NAME} \
        There specified by 'tag' column from Pixiv metadata table."
        
    )

    parser.add_argument(
        "--db-path",
        required=True,
        type=str,
        help="absolute or relative path of database that contains Pixiv metadata table to add 'tag_character' column"
    )

    parser.add_argument(
        "--aliase-path",
        required=True,
        type=str,
        help=f"absolute or relative path of database that contains aliase table {DEFAULT_ALIASE_TABLE_NAME}"
    )

    args = parser.parse_args()

    add_tag_character_column(args.db_path, args.aliase_path)
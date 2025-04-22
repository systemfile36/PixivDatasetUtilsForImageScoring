import sqlite3
import pandas as pd
import numpy as np
import os
import argparse
from tqdm import tqdm

from logger_factory import get_file_handler, get_default_stream_handler, get_custom_handlers_logger

# settings
TABLE_NAME = "illusts"
SAMPLE_SIZE = 20000
BIN_COUNT = 20

logger_handlers = [
    get_file_handler(log_prefix=os.path.basename(__file__)),
    get_default_stream_handler()
]
logger = get_custom_handlers_logger(__file__, logger_handlers)

def sampling_bookmarks(db_path: str, 
                    sample_size: int = SAMPLE_SIZE, bin_count = BIN_COUNT):
    
    logger.info(f"Connect to database {db_path}...")

    # connet to db
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        # load all data from db
        df = pd.read_sql_query(f"""
            SELECT filename, total_bookmarks
            FROM {TABLE_NAME}
            WHERE total_bookmarks IS NOT NULL
        """, conn)
    except Exception as e:
        logger.error(f"error has occured : {e}")
        return

    # devide to distribution range
    counts, bin_edges = np.histogram(df['total_bookmarks'], bins=bin_count)

    # cut by bin_edges using pandas.cut
    df['bin'] = pd.cut(df['total_bookmarks'], bins=bin_edges, labels=False, include_lowest=True)

    # sampling per bin size
    samples_per_bin = sample_size // bin_count
    sampled_filenames = []

    # for each bin group
    for bin_id in range(bin_count):
        bin_group = df[df['bin'] == bin_id]

        """
        # if length of current bin gruop less than 60% of bin count per samples, 
        # don't add to sample
        if len(bin_group) <= samples_per_bin * 0.6:
            continue
        """

        n = min(len(bin_group), samples_per_bin)
        if n > 0:
            sampled = bin_group.sample(n, random_state=42)['filename'].tolist()
            sampled_filenames.extend(sampled)

    sampled_filenames_set = set(sampled_filenames)

    # get all id list and 
    all_filenames = df['filename'].tolist()

    #set calculation. it's fast
    to_delete_filenames = list(set(all_filenames) - sampled_filenames_set)

    logger.info(f"Records count : {len(all_filenames)}")
    logger.info(f"To delete records cound: {len(to_delete_filenames)}")
    logger.info(f"Sample records count: {len(sampled_filenames)}")

    # execute DELETE query
    with conn:
        # create temp table and insert all sampled filenames
        # temp table exists only while the connection is open
        conn.execute("CREATE TEMP TABLE samples (filename TEXT PRIMARY KEY)")
        conn.executemany("""
            INSERT INTO samples(filename) VALUES (?)
        """, [(fn, ) for fn in sampled_filenames])

        # delete with subquery that references temp table
        conn.execute(f"""
            DELETE FROM {TABLE_NAME} 
            WHERE filename NOT IN (
                SELECT filename FROM samples
            )
        """)

    # complete and save 
    conn.commit()

    # vacuum to compact size of db file
    try:
        logger.info("Vaccum db...")
        conn.execute("VACUUM;")
        logger.info("Vaccum db complete")
    except:
        logger.error("failed to vaccum!")
        return
    finally:
        conn.close()

    logger.info("Sampling complete")

def main():
    parser = argparse.ArgumentParser(
        description="Sampling metadata from sqlite3 database. WARNING: Sampling will modify database. "
    )

    parser.add_argument(
        "db_path",
        type=str,
        help="absolute or relative path of database file"
    )

    parser.add_argument(
        "--sample-size",
        type=int,
        help="size of sample"
    )

    parser.add_argument(
        "--bin-count",
        type=int,
        help="number of bins"
    )

    args = parser.parse_args()

    logger.debug(f"Input: db_path: {args.db_path}, sample-size:{args.sample_size}, bin-count:{args.bin_count}")

    kwargs = {}

    if args.sample_size is not None:
        kwargs['sample_size'] = args.sample_size

    if args.bin_count is not None:
        kwargs['bin_count'] = args.bin_count

    sampling_bookmarks(args.db_path, **kwargs)

if __name__ == "__main__":
    main()

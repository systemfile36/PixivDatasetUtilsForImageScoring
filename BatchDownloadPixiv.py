import json
import subprocess
import logger_factory
import urllib.parse
import argparse
import os
from datetime import datetime, timedelta

COMMAND_FORMAT = 'gallery-dl -D "{path}" \
--chunk-size "0.8m" --download-archive "{path}/.archive/archive.sqlite3" --range "{range}" --exec "python DeleteIfExists.py \"{path}\" \"{{_filename}}\" \"{{_path}}\"" \
--write-metadata "https://www.pixiv.net/tags/{tag}%20-%E6%BC%AB%E7%94%BB%20-%E8%AC%9B%E5%BA%A7%20-%E9%A2%A8%E6%99%AF/artworks?order={order}&scd={scd}&ecd={ecd}&s_mode={s_mode}"'

ORDER_BY = ("date", "date_d", "popular_d")

S_MODE = ("s_tag", "s_tag_full", "s_tc")

logger_handlers = [
    logger_factory.get_file_handler(log_prefix=os.path.basename(__file__)),
    logger_factory.get_default_stream_handler()
]
logger = logger_factory.get_custom_handlers_logger(__file__, logger_handlers)

def batch_download_pixiv_by_tags(
        path: str, tag_string: str | None=None,
        tag_list_file: str='pixiv_tags_top_500.json',
        order: str="popular_d",
        date_lower_bound: str='2020-04-04',
        date_upper_bound: str='2025-04-04',
        range_string: str='1-2000',
        s_mode: str="s_tag",
        skip_rows: int=0):
    """
    batch download by tags list (json)
    tags list's format should be like [{tag_name: ..., }, ...]
    """

    if tag_string:
        tags_list = [tag_string]
    else:
        try: 
            with open(tag_list_file, encoding='utf-8') as f:
                tags_list = json.load(f)
        except Exception as e:
            logger.warning(f"Can not read file {tag_list_file} - {e}")
            return

    # Count row to skip rows
    rows_count = 0

    for obj in tags_list:
        # If rows count less than skip rows, skip current iteration
        if rows_count < skip_rows:
            rows_count = rows_count + 1
            continue

        if tag_string:
            tag_name = obj
        else:
            try:
                tag_name = obj.get('tag_name')
            except Exception as e:
                logger.warning(f"Can not parse JSON file {tag_list_file} - {e}")
                return

        ecd = datetime.strptime(date_upper_bound, "%Y-%m-%d")
        scd = ecd.replace(year=ecd.year - 1)

        temp = datetime.strptime(date_lower_bound, "%Y-%m-%d")

        while scd.year >= temp.year:

            logger.info(f"download {tag_name}, scd: {scd.date()}, ecd: {ecd.date()}")

            cmd = COMMAND_FORMAT.format(path=path, range=range_string, tag=urllib.parse.quote(tag_name), order=order, scd=scd.date(), ecd=ecd.date(), s_mode=s_mode)

            logger.debug(f"Execute {cmd}")

            result = subprocess.run(cmd, capture_output=False, shell=True)

            if(result.returncode != 0):
                logger.warning(f"error occured executing {cmd}, stderr: {result.stderr}")

            ecd = ecd.replace(year=ecd.year - 1)
            scd = scd.replace(year=scd.year - 1)

if __name__ == '__main__':

    parser = argparse.ArgumentParser(
        description="Download Pixiv images by script using gallery-dl"
    )

    parser.add_argument(
        "path",
        type=str,
        help="directory path to download data"
    )

   
    parser.add_argument(
        "--keywords",
        type=str,
        help="Keywords string to search pixiv. if use this option, --tag-list will be ignored"
    )
    
    parser.add_argument(
        "--tag-list",
        type=str,
        help="Tag list JSON file. format is like [{'tag_name'='...'}, ...].\n" \
        "Default is pixiv_tags_top_500.json"
    ) 
    
    parser.add_argument(
        "--date-range",
        type=str,
        help="set range of date. space-seperated like YYYY-MM-DD YYYY-MM-DD\n" \
        "e.g.) '2024-04-04 2025-04-04'"
    )

    parser.add_argument(
        "--range",
        type=str,
        help="gallery-dl's --range option. e.g.) '1-2000'"
    )

    parser.add_argument(
        "--order-by",
        type=str,
        help="set order like popular_d..."
    )

    parser.add_argument(
        "--s-mode",
        type=str,
        help="Pixiv search mode value. e.g.) 's_tag', 's_tag_full'. \nDefault is 's_tag' (partial match for tag)"
    )

    parser.add_argument(
        "--skip-rows",
        type=int,
        help="Number of rows to skip. Skip n rows from tag list specified by '--tag-list' option. Default is 0"
    )

    args = parser.parse_args()

    logger.debug(args)

    # dict
    kwargs = {}

    if args.keywords is not None:
        kwargs['tag_string'] = args.keywords

    if args.tag_list is not None:
        kwargs['tag_list_file'] = args.tag_list

    if args.date_range is not None:
        try:
            lower, upper = args.date_range.split()
            kwargs['date_lower_bound'] = lower
            kwargs['date_upper_bound'] = upper
        except ValueError:
            logger.warning("Invalid date range format. Use 'YYYY-MM-DD YYYY-MM-DD'.")

    if args.range is not None:
        kwargs['range_string'] = args.range

    if args.order_by is not None and args.order_by in ORDER_BY:
        kwargs['order'] = args.order_by

    if args.skip_rows is not None:
        kwargs['skip_rows'] = args.skip_rows

    if args.s_mode is not None:
        kwargs['s_mode'] = args.s_mode

    batch_download_pixiv_by_tags(args.path, **kwargs)

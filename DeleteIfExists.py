import argparse
import os
import sys

import logger_factory

logger_handlers = [
    logger_factory.get_file_handler(log_prefix=os.path.basename(__file__)),
    logger_factory.get_default_stream_handler()
]
logger = logger_factory.get_custom_handlers_logger(__file__, logger_handlers)

def delete_if_exists(root_path: str, file_name: str, abs_path: str):
    """
    Function for postprocess of gallery-dl
    """

    target_path = os.path.join(root_path, file_name[:2], file_name[:-4] + ".png")

    logger.debug(f"Check {target_path}")

    if os.path.exists(target_path):
        if os.path.exists(abs_path):
            try:
                os.remove(abs_path)
                logger.debug(f"{abs_path} is deleted")
            except Exception as e:
                logger.debug(f"Failed to delete {abs_path}")
                sys.exit(1)

            if os.path.exists(abs_path + ".json"):
                try:
                    os.remove(abs_path + ".json")
                    logger.debug(f"{abs_path + ".json"} is deleted")
                except Exception as e:
                    logger.debug(f"Failed to delete {abs_path + ".json"}")
            else:
                logger.debug(f"{abs_path + ".json"} does not exists yet.")
        else:
            logger.debug(f"{abs_path} does not exists.")
    else: 
        logger.debug(f"{target_path} does not exists...")

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Delete file if already exists in sub-directory(file_name[:2])"
    )

    parser.add_argument(
        "root_path",
        type=str,
        help="root_path of download_directory"
    )

    parser.add_argument(
        "file_name",
        type=str,
        help="filename of downloaded. gallery-dl {_filename}"
    )

    parser.add_argument(
        "abs_path",
        type=str,
        help="absolute path of original file"
    )

    args = parser.parse_args()

    delete_if_exists(args.root_path, args.file_name, args.abs_path)
import os
import re
import sys
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed
import logger_factory

# Source directory path
#SOURCE_DIR = 'H:/PixivDataBookmarks'

# Maximum workers on theradpool
MAX_WORKERS = 8  

# Regular expression for matching filename
pattern = re.compile(r'^(?P<id>\d{6,10})_p\d+\.(jpg|png|zip)$')

def distribute_files_by_id(source_dir:str):
    """
    Distribute files by id prefix(id[:2])
    """

    logger = logger_factory.get_default_logger(__name__)
    
    # Collect file list 
    all_files = os.listdir(source_dir)

    # Define move file work
    def move_file(file_name: str, source_dir: str):
        try:
            # if JSON metadata file, base_name will be images name 
            base_name = file_name[:-5] if file_name.endswith('.json') else file_name
            match = pattern.match(base_name)
            
            if not match:
                return  # if ont match to regex, then ignore

            id_part = match.group("id")
            prefix = id_part[:2]
            target_dir = os.path.join(source_dir, prefix)

            #make directory if not exists (to prevent race condition)
            os.makedirs(target_dir, exist_ok=True)

            src_path = os.path.join(source_dir, file_name)
            dst_path = os.path.join(target_dir, file_name)

            shutil.move(src_path, dst_path)
        except Exception as e:
            logger.error(f"{file_name}: {e}")


    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(move_file, file, source_dir) for file in all_files]

        for future in as_completed(futures):
            pass  # wait for complete (and catch exception)

    logger.info("Distribute Complete!")

def restore_files_to_source(source_dir: str):
    """
    Restore files that distributed to source directory
    """

    logger = logger_factory.get_default_logger(__name__)
    
    tasks = []

    # Collect only subdirectory's entries
    for entry in os.listdir(source_dir):
        subdir_path = os.path.join(source_dir, entry)
        if os.path.isdir(subdir_path):
            for file_name in os.listdir(subdir_path):
                src_path = os.path.join(subdir_path, file_name)
                dst_path = os.path.join(source_dir, file_name)
                tasks.append((src_path, dst_path, subdir_path))

    def move_file(src_path, dst_path, subdir_path):
        
        try:
            if os.path.exists(dst_path):
                logger.warning(f"{os.path.basename(dst_path)} already exists in source. Skipping.")
                return
            shutil.move(src_path, dst_path)
        except Exception as e:
            logger.error(f"Failed to move {src_path}: {e}")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(move_file, src, dst, subdir) for src, dst, subdir in tasks]
        for future in as_completed(futures):
            pass  # wait for complete (and catch exception)

    # Delete empty directory after all tasks complete
    for entry in os.listdir(source_dir):
        subdir_path = os.path.join(source_dir, entry)
        if os.path.isdir(subdir_path) and not os.listdir(subdir_path):
            os.rmdir(subdir_path)

    logger.info("Restore Complete")

if __name__ == '__main__':

    if len(sys.argv) > 2:
        if(sys.argv[1] == '-d'):
            distribute_files_by_id(sys.argv[2])
        elif(sys.argv[1] == '-r'):
            restore_files_to_source(sys.argv[2])

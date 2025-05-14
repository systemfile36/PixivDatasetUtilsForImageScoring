import os
import lmdb
import json
import tarfile
import argparse
from pathlib import Path

from PIL import Image
from io import BytesIO

# Assume logger is defined elsewhere and injected into this script
logger = None  # Placeholder for external logger injection

def encode_image(file_path):
    """
    Opens and encodes an image file into bytes.
    This is used to store the image data into LMDB.
    """
    try:
        with Image.open(file_path) as img:
            with BytesIO() as buffer:
                img.save(buffer, format=img.format or 'PNG')
                logger.debug(f"Encoded image: {file_path}")
                return buffer.getvalue()
    except Exception as e:
        logger.warning(f"Failed to encode {file_path}: {e}")
        return None

def process_images_to_lmdb(dataset_root, lmdb_path, batch_size):
    """
    Stores images from the dataset directory into an LMDB database.
    Automatically removes original image files after each batch commit.

    Parameters:
    - dataset_root: Path object representing the dataset directory
    - lmdb_path: Path object where the LMDB file will be created
    - batch_size: Number of images to process per commit
    """
    try:
        logger.info(f"Opening LMDB environment at {lmdb_path} with map size of 1TB")
        env = lmdb.open(str(lmdb_path), map_size=1 << 40)
    except Exception as e:
        logger.error(f"Failed to open LMDB environment: {e}")
        return

    try:
        image_files = list(dataset_root.rglob("*.png")) + list(dataset_root.rglob("*.jpg"))
        logger.info(f"Found {len(image_files)} image files to process")

        count = 0
        txn = env.begin(write=True)

        for img_path in image_files:
            try:
                key = img_path.name.encode()  # Use only the file name as the LMDB key
                value = encode_image(img_path)

                if value is not None:
                    txn.put(key, value)
                    logger.debug(f"Inserted {img_path.name} into LMDB")
                    count += 1

                    if count % batch_size == 0:
                        txn.commit()
                        logger.info(f"Committed {count} images to LMDB")

                        for i in range(count - batch_size, count):
                            try:
                                os.remove(image_files[i])
                                logger.debug(f"Deleted original image file: {image_files[i]}")
                            except Exception as e:
                                logger.warning(f"Could not remove {image_files[i]}: {e}")

                        txn = env.begin(write=True)
            except Exception as e:
                logger.error(f"Error processing file {img_path}: {e}")

        try:
            txn.commit()
            logger.info(f"Final commit completed: {count} total images")
        except Exception as e:
            logger.error(f"Failed to commit final batch: {e}")
            txn.abort()

        for i in range(count - (count % batch_size), count):
            try:
                os.remove(image_files[i])
                logger.debug(f"Deleted original image file: {image_files[i]}")
            except Exception as e:
                logger.warning(f"Could not remove {image_files[i]}: {e}")

    except Exception as e:
        logger.error(f"Unexpected error during LMDB processing: {e}")

    finally:
        env.close()
        logger.info("LMDB environment closed")

def archive_json_files(dataset_root):
    """
    Archives all .json files in the dataset directory into a single .tar file.
    This preserves metadata while reducing the file count.
    """
    try:
        json_files = list(dataset_root.rglob("*.json"))
        if not json_files:
            logger.info("No JSON files found to archive.")
            return

        archive_path = dataset_root / "metadata_jsons.tar"
        with tarfile.open(archive_path, "w") as tar:
            for json_file in json_files:
                try:
                    tar.add(json_file, arcname=json_file.relative_to(dataset_root))
                    logger.debug(f"Added {json_file} to archive")
                except Exception as e:
                    logger.warning(f"Failed to add {json_file} to archive: {e}")

        logger.info(f"Archived {len(json_files)} JSON files into {archive_path}")
    except Exception as e:
        logger.error(f"Failed to archive JSON files: {e}")

def main():
    parser = argparse.ArgumentParser(description="Migrate images to LMDB and archive metadata JSONs.")
    parser.add_argument("--dataset_root", type=Path, required=True, help="Path to dataset root directory")
    parser.add_argument("--lmdb_path", type=Path, required=True, help="Path to output LMDB file")
    parser.add_argument("--batch_size", type=int, default=1000, help="Number of images per LMDB commit batch")
    args = parser.parse_args()

    global logger
    # logger should be initialized and assigned externally

    try:
        logger.info("Starting LMDB migration process")
        process_images_to_lmdb(args.dataset_root, args.lmdb_path, args.batch_size)
        logger.info("Image migration completed. Starting metadata archiving.")
        archive_json_files(args.dataset_root)
        logger.info("Process completed successfully.")
    except Exception as e:
        logger.error(f"Fatal error in main execution: {e}")

if __name__ == "__main__":
    main()

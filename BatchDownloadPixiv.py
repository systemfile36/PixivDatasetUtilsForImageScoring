import json
import subprocess
import logger_factory
import urllib.parse
from datetime import datetime, timedelta

COMMAND_FORMAT = 'gallery-dl -D "H:/PixivDataBookmarks" --write-log "H:/PixivDataBookmarks/log.txt" \
--chunk-size "0.8m" --download-archive "H:/PixivDataBookmarks/.archive/archive.sqlite3" \
--write-metadata "https://www.pixiv.net/tags/{tag}%20-R-18%20-AI%E3%82%A4%E3%83%A9%E3%82%B9%E3%83%88%20-AI%E7%94%9F%E6%88%90%20-%E6%BC%AB%E7%94%BB%20-%E8%AC%9B%E5%BA%A7%20-%E9%A2%A8%E6%99%AF/artworks?order=popular_d&mode=safe&scd={scd}&ecd={ecd}&s_mode=s_tag"'

def batch_download_pixiv_by_tags():
    """
    batch download by tags list (json)
    tags list's format should be like [{tag_name: ..., }, ...]
    """
    #get logger
    logger = logger_factory.get_default_logger(__name__)

    with open('pixiv_tags_top_500.json', encoding='utf-8') as f:
        tags_list = json.load(f)

    for obj in tags_list:
        tag_name = obj.get('tag_name')

        ecd = datetime.strptime("2025-04-04", "%Y-%m-%d")
        scd = datetime.strptime("2024-04-04", "%Y-%m-%d")

        while scd.year >= 2020:

            logger.info(f"download {tag_name}, scd: {scd.date()}, ecd: {ecd.date()}")

            cmd = COMMAND_FORMAT.format(tag=urllib.parse.quote(tag_name), scd=scd.date(), ecd=ecd.date())

            logger.debug(f"Execute {cmd}")

            result = subprocess.run(cmd, capture_output=False, shell=True)

            if(result.returncode != 0):
                logger.warning(f"error occured executing {cmd}, stderr: {result.stderr}")

            ecd = ecd.replace(year=ecd.year - 1)
            scd = scd.replace(year=scd.year - 1)

if __name__ == '__main__':
    batch_download_pixiv_by_tags()
    subprocess.run("shutdown /s /t 60", shell=True)

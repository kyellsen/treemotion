# treemotion/tms_csv/wind_dwd_download.py

from pathlib import Path
from typing import Optional, Union, Tuple

import requests
from bs4 import BeautifulSoup
import zipfile
import re

from kj_core.utils.log_manager import get_logger
from kj_core.utils.path_utils import get_directory

logger = get_logger(__name__)

LINK_WIND: str = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/wind/recent/"
LINK_WIND_EXTREME: str = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/extreme_wind/recent/"
LINK_STATIONS_LIST: str = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/wind/recent/zehn_min_ff_Beschreibung_Stationen.txt"


def download_dwd_files(stations_id: str, directory: Union[str, Path], link_wind: str = LINK_WIND,
                       link_wind_extreme: str = LINK_WIND_EXTREME,
                       link_stations_list: str = LINK_STATIONS_LIST) -> Tuple[
    Optional[str], Optional[str], Optional[str]]:
    """
    Download and extract data files from DWD and return their paths.

    :param stations_id: the id of the station
    :param directory: the folder where the files will be saved
    :param link_wind: the url of the wind data
    :param link_wind_extreme: the url of the wind extreme data
    :param link_stations_list: the url of the stations list
    :return: Tuple containing paths to wind data file, wind extreme data file, and stations list file
    """
    directory = get_directory(directory)

    filename_wind = None
    filename_wind_extreme = None

    # Download wind and wind extreme data files
    for link in [link_wind, link_wind_extreme]:
        try:
            response = requests.get(link)
            soup = BeautifulSoup(response.text, 'html.parser')
            file = soup.find('a', href=re.compile(f"{stations_id}_akt.zip"))
            if file is not None:
                url = link + file['href']
                filename = download_wind_files(url, directory, stations_id)
                if link == link_wind:
                    filename_wind = filename
                else:
                    filename_wind_extreme = filename
            else:
                logger.warning(f"No file found for station {stations_id} at {link}")
        except Exception as e:
            logger.error(f"Error downloading file from {link}: {e}")

    # Download stations list file
    try:
        filename_stations_list = download_stations_list_file(link_stations_list, directory)
    except Exception as e:
        logger.error(f"Error downloading stations list file: {e}")
        filename_stations_list = None

    return filename_wind, filename_wind_extreme, filename_stations_list


def download_wind_files(url, folder_path, stations_id):
    """
    Download a zip file from a given url, extract it in a given folder, and then delete_from_db the zip file.

    :param url: the url of the zip file
    :param folder_path: the folder where the zip file will be extracted
    :param stations_id: the id of the station
    :return filename: the name of the extracted file
    """
    r = requests.get(url, stream=True)
    folder_path = Path(folder_path)
    zip_path = folder_path / f"{stations_id}_akt.zip"
    with zip_path.open('wb') as f:
        f.write(r.content)
    logger.info(f"ZIP file {stations_id}_akt.zip downloaded at {folder_path}")

    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(folder_path)
        filename = zip_ref.namelist()[0]
    logger.info(f"ZIP file {stations_id}_akt.zip extracted, TXT file {filename} saved at {folder_path}")

    zip_path.unlink()
    logger.info(f"ZIP file {stations_id}_akt.zip deleted from {folder_path}")

    return filename


def download_stations_list_file(url, folder_path):
    """
    Download a text file from a given url and save it in a given folder.

    :param url: the url of the text file
    :param folder_path: the folder where the text file will be saved
    :return filename: the name of the downloaded file
    """
    response = requests.get(url)
    filename = url.split('/')[-1]
    folder_path = Path(folder_path)
    txt_path = folder_path / filename
    with txt_path.open('wb') as f:
        f.write(response.content)
    logger.info(f"TXT file {filename} downloaded at {folder_path}")

    return filename

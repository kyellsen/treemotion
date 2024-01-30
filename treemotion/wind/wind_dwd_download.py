from pathlib import Path
from typing import Optional, Union

import requests
from bs4 import BeautifulSoup
import zipfile
import re

from kj_logger import get_logger
from kj_core.utils.path_utils import get_directory

logger = get_logger(__name__)

LINK_WIND = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/wind/recent/"
LINK_WIND_EXTREME = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/extreme_wind/recent/"
LINK_STATIONS_LIST = "https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/wind/recent/zehn_min_ff_Beschreibung_Stationen.txt"


def download_wind_file(stations_id: str, directory: Union[str, Path]) -> Optional[str]:
    """
    Download and extract the wind data file from DWD for a specific station.
    """
    try:
        directory = get_directory(directory)
        filename = download_and_extract_zip(LINK_WIND, stations_id, directory)
        if filename:
            logger.info(f"Wind file for station {stations_id} downloaded and extracted to {directory}")
        else:
            logger.warning(f"Wind file for station {stations_id} not found or failed to download.")
        return filename
    except Exception as e:
        logger.error(f"Error in downloading wind file for station {stations_id}: {e}")
        return None


def download_wind_extreme_file(stations_id: str, directory: Union[str, Path]) -> Optional[str]:
    """
    Download and extract the wind extreme data file from DWD for a specific station.
    """
    try:
        directory = get_directory(directory)
        filename = download_and_extract_zip(LINK_WIND_EXTREME, stations_id, directory)
        if filename:
            logger.info(f"Wind extreme file for station {stations_id} downloaded and extracted to {directory}")
        else:
            logger.warning(f"Wind extreme file for station {stations_id} not found or failed to download.")
        return filename
    except Exception as e:
        logger.error(f"Error in downloading wind extreme file for station {stations_id}: {e}")
        return None


def download_station_list_file(directory: Union[str, Path]) -> Optional[str]:
    """
    Download the stations list file from DWD.
    """
    try:
        directory = get_directory(directory)
        filename = download_text_file(LINK_STATIONS_LIST, directory)
        if filename:
            logger.info(f"Stations list file downloaded to {directory}")
        else:
            logger.warning(f"Stations list file failed to download.")
        return filename
    except Exception as e:
        logger.error(f"Error in downloading stations list file: {e}")
        return None


def download_and_extract_zip(link: str, stations_id: str, directory: Path) -> Optional[str]:
    """
    Download a ZIP file from a given URL, extract it, and return the name of the extracted file.
    """
    try:
        response = requests.get(link)
        soup = BeautifulSoup(response.text, 'html.parser')
        file = soup.find('a', href=re.compile(f"{stations_id}_akt.zip"))
        if file:
            url = link + file['href']
            filename = download_zip_file(url, directory, stations_id)
            return filename
        else:
            logger.warning(f"No file found for station {stations_id} at {link}")
            return None
    except Exception as e:
        logger.error(f"Error downloading file from {link}: {e}")
        return None


def download_zip_file(url: str, folder_path: Path, stations_id: str) -> str:
    """
    Download a zip file from a given URL, extract it, and delete the zip file.
    """
    r = requests.get(url, stream=True)
    zip_path = folder_path / f"{stations_id}_akt.zip"
    with zip_path.open('wb') as f:
        f.write(r.content)

    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(folder_path)
        filename = zip_ref.namelist()[0]

    zip_path.unlink()
    return filename


def download_text_file(url: str, folder_path: Path) -> str:
    """
    Download a text file from a given URL and save it.
    """
    response = requests.get(url)
    filename = url.split('/')[-1]
    txt_path = folder_path / filename
    with txt_path.open('wb') as f:
        f.write(response.content)

    return filename

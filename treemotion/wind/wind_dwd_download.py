# treemotion/utils/wind_dwd_download.py

from pathlib import Path

import requests
from bs4 import BeautifulSoup
import zipfile
import re

from utils.path_utils import get_directory
from utils.log import get_logger

logger = get_logger(__name__)


def download_dwd_files(stations_id, directory, link_wind, link_wind_extreme, link_stations_liste):
    """
    Download and extract data files from DWD and return their names.

    :param stations_id: the id of the station
    :param directory: the folder where the files will be saved
    :param link_wind: the url of the wind data
    :param link_wind_extreme: the url of the wind extreme data
    :param link_stations_liste: the url of the stations list
    :return: the path of the folder and the names of the files
    """
    get_directory(directory)

    links = [link_wind, link_wind_extreme]
    filenames = []

    for link in links:
        response = requests.get(link)
        soup = BeautifulSoup(response.text, 'html.parser')
        file = soup.find('a', href=re.compile(f"{stations_id}_akt.zip"))
        if file is not None:
            url = link + file['href']
            filename = download_wind_files(url, directory, stations_id)
            filenames.append(filename)
        else:
            logger.warning(f"No file found for station {stations_id} at {link}")
            filenames.append(None)

    filename = download_stations_list_file(link_stations_liste, directory)
    filenames.append(filename)

    return directory, filenames[0], filenames[1], filenames[2]


def download_wind_files(url, folder_path, stations_id):
    """
    Download a zip file from a given url, extract it in a given folder, and then delete the zip file.

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

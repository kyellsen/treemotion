# treemotion/utilities/dwd_download.py

from pathlib import Path

import requests
from bs4 import BeautifulSoup
import zipfile
import re

from utilities.log import get_logger


logger = get_logger(__name__)

def download_dwd_txt(stations_id, folder_path, link_1=None, link_2=None):
    links = [link_1, link_2]
    filenames = []
    folder_path = Path(folder_path)
    folder_path.mkdir(parents=True, exist_ok=True)  # Erstellen des Verzeichnisses, wenn es nicht existiert

    for link in links:
        # Anforderung der Website und Erstellung des Soup-Objekts
        response = requests.get(link)
        soup = BeautifulSoup(response.text, 'html.parser')

        # Suche nach der ZIP-Datei mit der passenden Stations-ID
        file = soup.find('a', href=re.compile(f"{stations_id}_akt.zip"))
        if file is not None:
            # Herunterladen und Speichern der ZIP-Datei
            url = link + file['href']
            r = requests.get(url, stream=True)
            zip_path = folder_path / f"{file['href']}"
            with zip_path.open('wb') as f:
                f.write(r.content)
            logger.info(f"ZIP file {file['href']} downloaded at {folder_path}")

            # Entpacken der ZIP-Datei
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(folder_path)
                # Speichern des Namens der extrahierten TXT-Datei
                filename = zip_ref.namelist()[0]
                filenames.append(filename)
                logger.info(f"ZIP file {file['href']} extracted, TXT file {filename} saved at {folder_path}")

            # Löschen der ZIP-Datei
            zip_path.unlink()
            logger.info(f"ZIP file {file['href']} deleted from {folder_path}")
        else:
            logger.warning(f"No file found for station {stations_id} at {link}")
            filenames.append(None)

    return folder_path, filenames[0], filenames[1]

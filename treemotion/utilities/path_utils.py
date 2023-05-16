# treemotion/utilities/path_utils.py

from pathlib import Path

from typing import List
from utilities.log import get_logger

logger = get_logger(__name__)


def validate_and_get_path(path):
    """
    Überprüft, ob der eingegebene Pfad ein gültiges Path-Objekt ist oder in ein solches umgewandelt werden kann.
    Wenn die Umwandlung erfolgreich ist, wird das Path-Objekt zurückgegeben, ansonsten wird None zurückgegeben.

    :param path: Ein String oder Path-Objekt, das den zu überprüfenden Pfad repräsentiert.
    :return: Ein gültiges Path-Objekt oder None.
    """
    if path is None:
        logger.warning(f"Filepath = None, Prozess abgebrochen.")
        return None

    try:
        path = Path(path)
    except TypeError as e:
        logger.error(f"Input kann nicht in ein Path-Objekt umgewandelt werden. Fehler: {e}")
        return None

    return path


def validate_and_get_filepath(filepath):
    """
    Überprüft, ob der angegebene Pfad zu einer existierenden Datei führt.

    :param filepath: Ein String oder Path-Objekt, das den zu überprüfenden Pfad repräsentiert.
    :return: Ein gültiges Path-Objekt oder None, wenn der Pfad nicht zu einer existierenden Datei führt.
    """
    filepath = validate_and_get_path(filepath)
    if filepath is None:
        logger.warning(f"Filepath = None, Prozess abgebrochen.")
        return None

    if not filepath.is_file():
        logger.error(f"Die Datei {filepath} existiert nicht.")
        return None
    return filepath


def validate_and_get_file_list(search_path: Path) -> List[Path]:
    """
    Sucht nach CSV-Dateien im angegebenen Pfad und gibt diese als Liste zurück.

    :param search_path: Der Pfad, in dem gesucht werden soll
    :return: Liste mit Pfaden zu den gefundenen CSV-Dateien
    """
    files = list(search_path.glob('**/*.csv'))

    if not files:
        logger.warning(f"Keine CSV-Dateien in Pfad gefunden: {search_path}")
        return None

    logger.info(f"Gefundene Dateien: {[file.stem for file in files]}")
    return files


def extract_id_sensor_list(files: List[Path]) -> List[int]:
    """
    Extrahiert die Sensor-ID aus den Dateinamen und gibt diese als Liste zurück.

    :param files: Liste von Dateipfaden
    :return: Liste mit Sensor-IDs oder bei Fehler None
    """
    try:
        id_sensor_list = [int(filename.stem[-3:]) for filename in files]
    except ValueError as e:
        logger.error(f"Extraktion der Sensor-ID aus Dateinamen fehlgeschlagen. Fehler: {e}")
        return None

    logger.info(f"Extrahierte Sensor-IDs: {id_sensor_list}")
    return id_sensor_list

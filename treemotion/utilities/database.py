# treemotion/utilities/database.py

import shutil

from utilities.common_imports import *


def new_db(path, name):
    path = Path(path)
    filename = f"{name}.db"
    path_db = path / filename

    if path_db.exists():
        logger.error(f"Fehler: Eine Datenbank mit dem Namen '{filename}' existiert bereits in {path}.")
        return None

    template_db_name = configuration.template_db_name
    current_file = Path(__file__)
    parent_directory = current_file.parent.parent
    template_db_path = parent_directory / template_db_name

    if not template_db_path.exists() or not template_db_path.is_file():
        logger.error(f"Fehler: Die {template_db_name}-Datei wurde nicht gefunden.")
        return None

    try:
        shutil.copy(template_db_path, path_db)
        logger.info(f"Datenbank '{filename}' wurde erfolgreich in {path} erstellt.")
        return str(path_db)
    except Exception as e:
        logger.error(f"Fehler beim Erstellen der Datenbank {filename}: {e}")
        return None



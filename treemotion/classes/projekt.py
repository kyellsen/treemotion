
# treemotion/classes/projekt.py
from pathlib import Path
import shutil

from treemotion import configuration
from .messreihe import Messreihe
from utilities.session import connect_to_database
from utilities.timing import timing_decorator

from utilities.log import get_logger


logger = get_logger(__name__)

class Projekt:

    def __init__(self):
        self.session = None
        self.id = int()
        self.name = str()
        self.path = Path()
        self.path_db = None
        self.messreihen_list = []

    @classmethod
    @timing_decorator
    def create(cls, id_projekt: int, name: str, path: str):
        obj = cls()
        obj.id = id_projekt
        obj.name = name
        obj.path = Path(path)

        obj.path.mkdir(parents=True, exist_ok=True)
        obj.path_db = obj.path / f"{name}.db"

        template_db_name = configuration.template_db_name
        current_file = Path(__file__)
        parent_directory = current_file.parent.parent
        template_db_path = parent_directory / template_db_name

        if not template_db_path.exists() or not template_db_path.is_file():
            logger.error(f"Fehler: Die {template_db_name}-Datei wurde nicht gefunden.")
            return None

        try:
            shutil.copy(template_db_path, obj.path_db)
        except Exception as e:
            logger.error(f"Fehler beim Kopieren der {template_db_name}-Datei: {e}")
            return None

        obj.session, error_message = connect_to_database(obj.path_db)
        if error_message:
            return error_message
        logger.info(f"Projekt '{name}' wurde erfolgreich erstellt.")
        return obj

    @classmethod
    @timing_decorator
    def from_database(cls, id_projekt: int, name: str, path: str):
        obj = cls()
        obj.id = id_projekt
        obj.name = name
        obj.path = Path(path)
        obj.path_db = obj.path / f"{name}.db"

        if not obj.path_db.exists() or not obj.path_db.is_file():
            logger.warning(f"Fehler: Die angegebene Datenbank {obj.path_db} wurde nicht gefunden.")
            return None

        obj.session, error_message = connect_to_database(obj.path_db)
        if error_message:
            return error_message
        logger.info(f"Projekt '{name}' wurde erfolgreich geladen.")
        return obj

    @timing_decorator
    def add_messreihe(self, id_messreihe):
        if any(messreihe.id_messreihe == id_messreihe for messreihe in self.messreihen_list):
            logger.warning(f"Messreihe mit ID {id_messreihe} bereits hinzugefügt")
            return

        with self.session as session:
            db_messreihe = session.query(Messreihe).filter_by(id_messreihe=id_messreihe).first()

        if db_messreihe is None:
            logger.error(f"Messreihe mit ID {id_messreihe} nicht gefunden")
            return

        messreihe = Messreihe.from_database(db_messreihe=db_messreihe, session=self.session)
        messreihe.add_messungen()
        self.messreihen_list.append(messreihe)
        logger.info(f"Messreihe {messreihe.id_messreihe}: erfolgreich hinzugefügt.")
        return

    @timing_decorator
    def add_messreihen(self):
        with self.session as session:
            db_messreihen = session.query(Messreihe).all()

        for db_messreihe in db_messreihen:
            if any(messreihe.id_messreihe == db_messreihe.id_messreihe for messreihe in self.messreihen_list):
                logger.warning(f"Messreihe mit ID {db_messreihe.id_messreihe} bereits hinzugefügt")
                continue

            messreihe = Messreihe.from_database(db_messreihe=db_messreihe, session=self.session)
            messreihe.add_messungen()
            self.messreihen_list.append(messreihe)
            logger.info(f"Messreihe '{messreihe.id_messreihe}': erfolgreich hinzugefügt.")

    @timing_decorator
    def add_filenames(self, csv_path):
        for messreihe in self.messreihen_list:
            try:
                messreihe.add_filenames(csv_path)
                logger.info(f"Messreihe {messreihe.id_messreihe}: Füge Filenames hinzu.")
            except Exception as e:
                logger.error(
                    f"Messreihe {messreihe.id_messreihe}: Fehler beim Hinzufügen der Filenames zu Messreihe. : {e}")

    @timing_decorator
    def add_data_from_csv(self, version=configuration.data_version_default):
        for messreihe in self.messreihen_list:
            try:
                messreihe.add_data_from_csv(version=version)
                logger.info(f"CSV-Daten für Messreihe {messreihe.id_messreihe} wurden erfolgreich hinzugefügt.")
            except Exception as e:
                logger.warning(f"Fehler beim Hinzufügen von Daten aus CSV für Messreihe {messreihe.id_messreihe}: {e}")

    @timing_decorator
    def add_data_from_db(self, version: str):
        for messreihe in self.messreihen_list:
            try:
                messreihe.add_data_from_db(version=version)
                logger.info(f"DB-Daten für Messreihe {messreihe.id_messreihe} wurden erfolgreich hinzugefügt.")
            except Exception as e:
                logger.warning(f"Fehler beim Hinzufügen von Daten aus DB für Messreihe {messreihe.id_messreihe}: {e}")

    @timing_decorator
    def delete_data_version_from_db(self, version):
        for messreihe in self.messreihen_list:
            try:
                messreihe.delete_data_version_from_db(version)
                logger.info(f"Daten-Version {version} für Messreihe {messreihe.id_messreihe} wurde erfolgreich gelöscht.")
            except Exception as e:
                logger.error(f"Fehler beim Löschen der Daten-Version {version} für Messreihe {messreihe.id_messreihe}: {e}")

    @classmethod
    @timing_decorator
    def load_complete(cls, id_projekt: int, name: str, path: str, csv_path: str):
        obj = cls.from_database(id_projekt, name, path)
        obj.add_messreihen()
        obj.add_filenames(csv_path=csv_path)
        obj.add_data_from_csv()
        return obj

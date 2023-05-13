# treemotion/classes/projekt.py

# import packages
import shutil

# import utilities
from utilities.common_imports import *

# import classes
from .base_class import BaseClass
from .messreihe import Messreihe


class Projekt(Base):
    __tablename__ = 'Projekt'
    id_projekt = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    name = Column(String)

    messreihen_list = relationship("Messreihe", backref="projekt", lazy='select')

    def __init__(self, path_db, id_projekt=None, name=None):
        self.path_db = path_db

        # in SQLite Database
        self.id_projekt = id_projekt
        self.name = name

    @classmethod
    @timing_decorator
    def from_database(cls, path_db, load_related=configuration.projekt_load_related_default):
        session = create_session(path_db)
        if load_related:
            objs = session.query(cls).options(joinedload(cls.messreihen_list)).all()
        else:
            objs = session.query(cls).all()
        session.close()
        logger.info(f"{len(objs)} Projekte wurden erfolgreich geladen.")
        return objs

    # @classmethod
    # @timing_decorator
    # def create(cls, id_projekt: int, name: str, path: str):
    #     obj = cls()
    #     obj.id = id_projekt
    #     obj.name = name
    #     obj.path = Path(path)
    #
    #     obj.path.mkdir(parents=True, exist_ok=True)
    #     obj.path_db = obj.path / f"{name}.db"
    #
    #     template_db_name = configuration.template_db_name
    #     current_file = Path(__file__)
    #     parent_directory = current_file.parent.parent
    #     template_db_path = parent_directory / template_db_name
    #
    #     if not template_db_path.exists() or not template_db_path.is_file():
    #         logger.error(f"Fehler: Die {template_db_name}-Datei wurde nicht gefunden.")
    #         return None
    #
    #     try:
    #         shutil.copy(template_db_path, obj.path_db)
    #     except Exception as e:
    #         logger.error(f"Fehler beim Kopieren der {template_db_name}-Datei: {e}")
    #         return None
    #
    #     obj.session, error_message = create_session(obj.path_db)
    #     if error_message:
    #         return error_message
    #     logger.info(f"Projekt '{name}' wurde erfolgreich erstellt.")
    #     return obj
    #
    # @timing_decorator
    # def add_filenames(self, csv_path):
    #     for messreihe in self.messreihen_list:
    #         try:
    #             messreihe.add_filenames(csv_path)
    #             logger.info(f"Messreihe {messreihe.id_messreihe}: Füge Filenames hinzu.")
    #         except Exception as e:
    #             logger.error(
    #                 f"Messreihe {messreihe.id_messreihe}: Fehler beim Hinzufügen der Filenames zu Messreihe. : {e}")
    #
    # @timing_decorator
    # def add_data_from_csv(self, version=configuration.data_version_default):
    #     for messreihe in self.messreihen_list:
    #         try:
    #             messreihe.add_data_from_csv(version=version)
    #             logger.info(f"CSV-Daten für Messreihe {messreihe.id_messreihe} wurden erfolgreich hinzugefügt.")
    #         except Exception as e:
    #             logger.warning(f"Fehler beim Hinzufügen von Daten aus CSV für Messreihe {messreihe.id_messreihe}: {e}")

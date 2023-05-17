# treemotion/classes/messreihe.py
from utilities.imports_classes import *

from utilities.path_utils import validate_and_get_path, validate_and_get_file_list, extract_id_sensor_list

from .messung import Messung

logger = get_logger(__name__)

class Messreihe(BaseClass):
    __tablename__ = 'Messreihe'
    id_messreihe = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    id_projekt = Column(Integer, ForeignKey('Projekt.id_projekt'))
    beschreibung = Column(String)
    datum_beginn = Column(DateTime)
    datum_ende = Column(DateTime)
    ort = Column(String)
    anmerkung = Column(String)
    filepath_tms = Column(String)

    messungen = relationship("Messung", backref="messreihe", lazy="joined", cascade='all, delete, delete-orphan',
                             order_by="Messung.id_messung")

    def __init__(self, *args, id_messreihe=None, beschreibung=None, datum_beginn=None, datum_ende=None, ort=None,
                 anmerkung=None, filepath_tms=None, **kwargs):
        super().__init__(*args, **kwargs)

        # in SQLite Database
        self.id_messreihe = id_messreihe
        self.beschreibung = beschreibung
        self.datum_beginn = datum_beginn
        self.datum_ende = datum_ende
        self.ort = ort
        self.anmerkung = anmerkung
        self.filepath_tms = filepath_tms

    @classmethod
    @timing_decorator
    def load_from_db(cls, id_projekt=None, session=None):
        objs = super().load_from_db(filter_by={'id_projekt': id_projekt} if id_projekt else None, session=session)
        logger.info(f"{len(objs)} Messreihen wurden erfolgreich geladen.")
        return objs

    @timing_decorator
    def remove(self, id_projekt='id_messreihe', auto_commit=False, session=None):
        session = db_manager.get_session(session)
        # Call the base class method to remove this Data object from the database
        super().remove(id_projekt, auto_commit, session)

    @timing_decorator
    def copy(self, id_name="id_messreihe", reset_id=False, auto_commit=False, session=None):
        new_instance = super().copy(id_name, reset_id, auto_commit, session)
        return new_instance

    def copy_deep(self, copy_relationships=True):
        copy = super().copy_deep(copy_relationships=copy_relationships)
        return copy

    @timing_decorator
    def add_filenames(self, csv_path: str):
        """
        Aktualisiert die Attribute 'filename' und 'filepath' für jede Messung dieser Messreihe,
        indem CSV-Dateien im angegebenen Pfad gesucht und deren Namen und Pfade extrahiert werden.

        :param csv_path: Der Pfad, in dem nach CSV-Dateien gesucht werden soll
        """
        csv_path = validate_and_get_path(csv_path)
        if csv_path is None:
            return None

        if self.filepath_tms is None:
            logger.warning(f"Kein filepath_tms in Messreihe {self.id_messreihe}")
            return None

        filepath_tms = validate_and_get_path(self.filepath_tms)
        if filepath_tms is None:
            return None

        search_path = csv_path.joinpath(filepath_tms)
        if not search_path.exists():
            logger.error(f"Suchpfad existiert nicht: {search_path}")
            return None

        logger.info(f"Suche TMS-Dateien in Pfad: {search_path}")

        files = validate_and_get_file_list(search_path)
        if files is None:
            return None

        id_sensor_list = extract_id_sensor_list(files)
        if id_sensor_list is None:
            return None

        for messung in self.messungen:
            if messung.id_sensor in id_sensor_list:
                corresponding_file = next((file for file in files if int(file.stem[-3:]) == messung.id_sensor), None)
                if corresponding_file and corresponding_file.is_file():
                    messung.filename = corresponding_file.name
                    messung.filepath = str(corresponding_file)
                else:
                    logger.error(f"Die Datei {corresponding_file} existiert nicht oder ist keine CSV-Datei.")
                    return None

        logger.info(f"Die Attribute filename und filepath wurden erfolgreich aktualisiert.")

    @timing_decorator
    def load_data_from_csv(self, session, version=configuration.data_version_default, overwrite=False):
        self.for_all('messungen', 'load_data_from_csv', session, version, overwrite)

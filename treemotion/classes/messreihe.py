# treemotion/classes/messreihe.py
from utils.imports_classes import *

from utils.path_utils import validate_and_get_path, validate_and_get_file_list, extract_id_sensor_list

from tms.time_utils import limit_df_by_time, optimal_time_frame
from tms.find_peaks import find_n_peaks

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

    def __str__(self):
        return f"Messreihe(id={self.id_messreihe}, id_messreihe={self.id_messreihe}"

    @classmethod
    @timing_decorator
    def load_from_db(cls, id_projekt=None, session=None):
        objs = super().load_from_db(filter_by={'id_projekt': id_projekt} if id_projekt else None, session=session)
        logger.info(f"{len(objs)} Messreihen wurden erfolgreich geladen.")
        return objs

    @timing_decorator
    def load_data_from_csv(self, version=config.default_load_data_from_csv_version_name, overwrite=False,
                           auto_commit=False,
                           session=None):
        logger.info(f"Starte Prozess zum laden aller CSV-Files für {self.__str__()}")
        try:
            results = self.for_all('messungen', 'load_data_from_csv', version, overwrite, auto_commit, session)
        except Exception as e:
            logger.error(f"Fehler beim Laden aller CSV-Files für {self.__str__()}, Error: {e}")
            return None
        logger.info(
            f"Prozess zum laden von CSV-Files für {len(results)} Messungen aus {self.__str__()} erfolgreich abgeschlossen.")
        return results

    @timing_decorator
    def copy(self, reset_id=False, auto_commit=False, session=None):
        new_obj = super().copy("id_messreihe", reset_id, auto_commit, session)
        return new_obj

    @timing_decorator
    def remove(self, auto_commit=False, session=None):
        session = db_manager.get_session(session)
        # Call the base class method to remove this Data object from the database
        result = super().remove('id_messreihe', auto_commit, session)
        return result

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
    def get_data_by_version(self, version):
        try:
            results = self.for_all('messungen', 'get_data_by_version', version)
        except Exception as e:
            logger.error(
                f"Fehler beim Suchen der Data-Instanzen mit Version '{version}' aus {self.__str__()}, Error: {e}")
            return None
        return results

    @timing_decorator
    def load_data_by_version(self, version, session=None):
        logger.info(f"Starte Prozess zum Laden der Data-Frames in {self.__str__()} mit Version: {version}")
        try:
            results = self.for_all('messungen', 'load_data_by_version', version, session)
        except Exception as e:
            logger.error(f"Fehler beim Laden der Data-Frames für {self.__str__()}, Error: {e}")
            return None
        logger.info(
            f"Prozess zum Laden der Data-Frames für {len(results)} Messungen aus {self.__str__()} erfolgreich abgeschlossen.")
        return results

    @timing_decorator
    def copy_data_by_version(self, version_new=config.default_copy_data_by_version_name,
                             version_source=config.default_load_data_from_csv_version_name, auto_commit=False,
                             session=None):
        logger.info(f"Starte Prozess zum kopieren aller Data-Objekte in {self.__str__()} mit Version: {version_source}")
        try:
            results = self.for_all('messungen', 'copy_data_by_version', version_new, version_source, auto_commit,
                                   session)
        except Exception as e:
            logger.error(f"Fehler beim Kopieren aller Data-Objekte für {self.__str__()}, Error: {e}")
            return None
        logger.info(
            f"Prozess zum Kopieren aller Data-Objekte für {len(results)} Messungen aus {self.__str__()} erfolgreich abgeschlossen.")
        return results

    @timing_decorator
    def commit_data_by_version(self, version, session=None):
        logger.info(f"Starte Prozess zum Commiten aller Data-Objekte in {self.__str__()} mit Version: {session}")
        try:
            results = self.for_all('messungen', 'commit_data_by_version', version, session)
        except Exception as e:
            logger.error(f"Fehler beim Commiten aller Data-Objekte für {self.__str__()}, Error: {e}")
            return False
        # Zählt die Anzahl der erfolgreichen Ergebnisse (die nicht False sind)
        successful = sum(1 for result in results if result is not False)
        logger.info(
            f"Prozess zum Commiten von Data-Objekten für {successful}/{len(results)} für Messungen aus {self.__str__()} erfolgreich.")
        return results

    @timing_decorator
    def limit_time_data_by_version(self, version, start_time: str, end_time: str, auto_commit: bool = False,
                                   session=None):
        logger.info(
            f"Starte Prozess zur Zeiteinschränkung aller Data-Objekte in {self.__str__()} mit Version: {version}")
        try:
            results = self.for_all('messungen', 'limit_time_data_by_version', version, start_time, end_time,
                                   auto_commit, session)
        except Exception as e:
            logger.error(f"Fehler bei Zeiteinschränkung aller Data-Objekte für {self.__str__()}, Error: {e}")
            return None
        # Zählt die Anzahl der erfolgreichen Ergebnisse (die nicht False sind)
        successful = sum(1 for result in results if result is not False)
        logger.info(
            f"Prozess zur Zeiteinschränkung von Data-Objekten für {successful}/{len(results)} für Messungen aus {self.__str__()} erfolgreich.")
        return results

    def limit_time_by_version_and_peaks(self, version, duration: int, show_peaks: bool = False,
                                        values_col: str = 'Absolute-Inclination - drift compensated',
                                        time_col: str = 'Time', n_peaks: int = 10, sample_rate: float = 20,
                                        min_time_diff: float = 60, prominence: int = None, auto_commit: bool = False,
                                        session=None):
        """
        Begrenzt die Zeiten basierend auf Peaks in den gegebenen Daten.
        """

        objs = self.get_data_by_version(version)
        peak_dicts = []

        for obj in objs:
            peaks = obj.find_n_peaks(show_peaks, values_col, time_col, n_peaks, sample_rate, min_time_diff, prominence)
            if peaks is None:
                continue
            peak_dicts.append(peaks)

        # Hier aus vielen peaks_dicts in Liste einen peaks_dict zusammensetzen
        merged_peaks = self.merge_peak_dicts(peak_dicts)
        try:
            timeframe_dict = optimal_time_frame(duration, merged_peaks)
        except Exception as e:
            logger.error(f"Optimaler Timeframe konnte nicht ermittelt werden, error: {e}")
            return False

        for obj in objs:
            obj.limit_by_time(timeframe_dict['start_time'], timeframe_dict['end_time'], auto_commit, session)

        return True

    @staticmethod
    def merge_peak_dicts(peak_dicts):
        """
        Führt eine Liste von 'peak' Wörterbüchern zusammen.
        """
        return {
            'peak_index': [index for peaks in peak_dicts for index in peaks['peak_index']],
            'peak_time': [time for peaks in peak_dicts for time in peaks['peak_time']],
            'peak_value': [value for peaks in peak_dicts for value in peaks['peak_value']]
        }

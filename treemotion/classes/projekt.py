# treemotion/classes/projekt.py

from utilities.imports_classes import *

from .messreihe import Messreihe

logger = get_logger(__name__)


class Projekt(BaseClass):
    __tablename__ = 'Projekt'
    id_projekt = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    name = Column(String)

    messreihen = relationship("Messreihe", backref="projekt", lazy="joined", cascade="all, delete, delete-orphan",
                              order_by="Messreihe.id_messreihe")

    def __init__(self, *args, id_projekt=None, name=None, **kwargs):
        super().__init__(*args, **kwargs)

        # in SQLite Database
        self.id_projekt = id_projekt
        self.name = name

    def __str__(self):
        return f"Projekt(id={self.id_projekt}, name={self.name}"

    @classmethod
    @timing_decorator
    def load_from_db(cls, id_projekt=None, session=None):
        session = db_manager.get_session(session)
        objs = super().load_from_db(filter_by={'id_projekt': id_projekt} if id_projekt else None, session=session)
        logger.info(f"{len(objs)} Projekte wurden erfolgreich geladen.")
        return objs

    @timing_decorator
    def load_data_from_csv(self, version=configuration.data_version_default, overwrite=False, auto_commit=False,
                              session=None):
        logger.info(f"Starte Prozess zum laden aller CSV-Files für {self.__str__()}")
        try:
            results = self.for_all('messreihen', 'load_data_from_csv', version, overwrite, auto_commit, session)
        except Exception as e:
            logger.error(f"Fehler beim Laden aller CSV-Files für {self.__str__()}, Error: {e}")
            return None
        logger.info(
            f"Prozess zum laden von CSV-Files für {len(results)} Messreihen aus {self.__str__()} erfolgreich abgeschlossen.")
        return results

    @timing_decorator
    def copy(self, reset_id=False, auto_commit=False, session=None):
        new_obj = super().copy("id_projekt", reset_id, auto_commit, session)
        return new_obj

    @timing_decorator
    def remove(self, auto_commit=False, session=None):
        session = db_manager.get_session(session)
        # Call the base class method to remove this Data object from the database
        super().remove('id_projekt', auto_commit, session)

    @timing_decorator
    def add_filenames(self, csv_path):
        self.for_all('messreihen', 'add_filenames', csv_path=csv_path)

    def get_data_by_version(self, version):
        try:
            results = self.for_all('messreihen', 'get_data_by_version', version)
        except Exception as e:
            logger.error(
                f"Fehler beim Suchen der Data-Instanzen mit Version '{version}' aus {self.__str__()}, Error: {e}")
            return None
        return results

    @timing_decorator
    def load_data_by_version(self, version, session=None):
        logger.info(f"Starte Prozess zum Laden der Data-Frames in {self.__str__()} mit Version: {version}")
        try:
            results = self.for_all('messreihen', 'load_data_by_version', version, session)
        except Exception as e:
            logger.error(f"Fehler beim Laden der Data-Frames für {self.__str__()}, Error: {e}")
            return None
        logger.info(
            f"Prozess zum Laden der Data-Frames für {len(results)} Messreihen aus {self.__str__()} erfolgreich abgeschlossen.")
        return results

    @timing_decorator
    def copy_data_by_version(self, version_new=configuration.data_version_copy_default,
                             version_source=configuration.data_version_default, auto_commit=False, session=None):
        logger.info(f"Starte Prozess zum kopieren aller Data-Objekte in {self.__str__()} mit Version: {version_source}")
        try:
            results = self.for_all('messreihen', 'copy_data_by_version', version_new, version_source, auto_commit,
                                   session)
        except Exception as e:
            logger.error(f"Fehler beim Kopieren aller Data-Objekte für {self.__str__()}, Error: {e}")
            return None
        logger.info(
            f"Prozess zum Kopieren aller Data-Objekte für {len(results)} Messreihen aus {self.__str__()} erfolgreich abgeschlossen.")
        return results

    @timing_decorator
    def commit_data_by_version(self, version, session=None):
        logger.info(f"Starte Prozess zum Commiten aller Data-Objekte in {self.__str__()} mit Version: {session}")
        try:
            results = self.for_all('messreihen', 'commit_data_by_version', version, session)
        except Exception as e:
            logger.error(f"Fehler beim Commiten aller Data-Objekte für {self.__str__()}, Error: {e}")
            return None
        # Zählt die Anzahl der erfolgreichen Ergebnisse (die nicht False sind)
        successful = sum(1 for result in results if result is not False)
        logger.info(
            f"Prozess zum Commiten von Data-Objekten für {successful}/{len(results)} für Messreihen aus {self.__str__()} erfolgreich.")
        return results

    @timing_decorator
    def limit_time_data_by_version(self, version, start_time: str, end_time: str, auto_commit: bool = False,
                                   session=None):
        logger.info(
            f"Starte Prozess zur Zeiteinschränkung aller Data-Objekte in {self.__str__()} mit Version: {version}")
        try:
            results = self.for_all('messreihen', 'limit_time_data_by_version', version, start_time, end_time,
                                   auto_commit, session)
        except Exception as e:
            logger.error(f"Fehler bei Zeiteinschränkung aller Data-Objekte für {self.__str__()}, Error: {e}")
            return None
        # Zählt die Anzahl der erfolgreichen Ergebnisse (die nicht False sind)
        successful = sum(1 for result in results if result is not False)
        logger.info(
            f"Prozess zur Zeiteinschränkung von Data-Objekten für {successful}/{len(results)} für Messreihen aus {self.__str__()} erfolgreich.")
        return results

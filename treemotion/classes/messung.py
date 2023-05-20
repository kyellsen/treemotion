# treemotion/classes/messung.py

from utilities.imports_classes import *

from .data import Data
from .baum import BaumBehandlung
from .sensor import Sensor, SensorOrt

logger = get_logger(__name__)


class Messung(BaseClass):
    __tablename__ = 'Messung'
    id_messung = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    id_messreihe = Column(Integer, ForeignKey('Messreihe.id_messreihe', onupdate='CASCADE'))
    id_baum_behandlung = Column(Integer, ForeignKey('BaumBehandlung.id_baum_behandlung', onupdate='CASCADE'))
    id_sensor = Column(Integer, ForeignKey('Sensor.id_sensor', onupdate='CASCADE'))
    id_messung_status = Column(Integer, ForeignKey('MessungStatus.id_messung_status', onupdate='CASCADE'))
    filename = Column(String)
    filepath = Column(String)
    id_sensor_ort = Column(Integer, ForeignKey('SensorOrt.id_sensor_ort', onupdate='CASCADE'))
    sensor_hoehe = Column(Integer)
    sensor_umfang = Column(Integer)
    sensor_ausrichtung = Column(Integer)

    baum_behandlung = relationship("BaumBehandlung", backref="messung", lazy="joined")
    sensor = relationship("Sensor", backref="messung", lazy="joined")
    messung_status = relationship("MessungStatus", backref="messung", lazy="joined")
    sensor_ort = relationship("SensorOrt", backref="messung", lazy="joined")

    data = relationship("Data", backref="messung", lazy="joined", cascade="all, delete, delete-orphan",
                        order_by="Data.id_data")

    def __init__(self, *args, id_messung: int = None, id_messreihe: int = None, id_baum_behandlung: int = None,
                 id_sensor: int = None, id_messung_status: int = None, filename: str = None, filepath: str = None,
                 id_sensor_ort: int = None, sensor_hoehe: int = None, sensor_umfang: int = None,
                 sensor_ausrichtung: int = None, **kwargs):
        """
        Initialisieren Sie eine neue Instanz der Messung-Klasse.

        :param id_messung: Die ID der Messung
        :param id_messreihe: Die ID der Messreihe
        :param id_baum_behandlung: Die ID der Baumbehandlung
        :param id_sensor: Die ID des Sensors
        :param id_messung_status: Der Status der Messung
        :param filename: Der Name der TMS.csv-Datei
        :param filepath: Der Pfad zur TMS.csv-Datei
        :param id_sensor_ort: Die ID des Sensororts
        :param sensor_hoehe: Die Höhe des Sensors
        :param sensor_umfang: Der Umfang des Baumstammes auf Sensorhoehe
        :param sensor_ausrichtung: Die Ausrichtung des Sensors (Grad gegen Nord)
        """
        super().__init__(*args, **kwargs)
        # in SQLite Database
        self.id_messung = id_messung
        self.id_messreihe = id_messreihe
        self.id_baum_behandlung = id_baum_behandlung
        self.id_sensor = id_sensor
        self.id_messung_status = id_messung_status
        self.filename = filename
        self.filepath = filepath
        self.id_sensor_ort = id_sensor_ort
        self.sensor_hoehe = sensor_hoehe
        self.sensor_umfang = sensor_umfang
        self.sensor_ausrichtung = sensor_ausrichtung

    def __str__(self):
        return f"Messung(id={self.id_messung}, id_messreihe={self.id_messreihe}, filename={self.filename}"

    # Geerbt von BaseClass
    @classmethod
    @timing_decorator
    def load_from_db(cls, id_messreihe=None, session=None):
        """
        Laden Sie Messungsobjekte aus der Datenbank basierend auf der ID der Messreihe.

        :param id_messreihe: Die ID der Messreihe, nach der gefiltert werden soll.
        :param session: Die SQL-Alchemie-Sitzung, die zum Interagieren mit der Datenbank verwendet wird.
        :return: Eine Liste von geladenen Messungsobjekten.
        """
        objs = super().load_from_db(filter_by={'id_messreihe': id_messreihe} if id_messreihe else None, session=session)
        if not objs:
            logger.error(f"Keine Daten gefunden für id_messreihe={id_messreihe}")
        else:
            logger.info(f"{len(objs)} Messung-Objekte wurden erfolgreich geladen.")
        return objs

    @timing_decorator
    def load_data_from_csv(self, version: str = configuration.data_version_default, overwrite: bool = False,
                              auto_commit: bool = False, session=None):
        """
        Lädt Daten aus einer CSV-Datei und aktualisiert ggf. bestehende Daten.

        :param version: Version der Daten.
        :param overwrite: Bestimmt, ob bestehende Daten überschrieben werden sollen.
        :param auto_commit: Bestimmt, ob die Daten sofort in der Datenbank gespeichert werden sollen.
        :param session: SQL-Alchemie-Session zur Interaktion mit der Datenbank.
        :return: Das aktualisierte oder neu erstellte Datenobjekt oder None, wenn ein Fehler auftritt.
        """
        if self.filepath is None:
            logger.warning(f"Process for '{self.__str__()}' aborted, no filename for tms.csv (filename = None).")
            return None

        logger.info(f"Start loading csv data for '{self.__str__()}'")
        table_name = Data.get_table_name(version, self.id_messung)
        present_data_obj = self.find_data_by_table_name(table_name)

        if present_data_obj is None or overwrite:
            if present_data_obj is not None and overwrite:
                logger.warning(f"Existing object will be overwritten (overwrite = True): {present_data_obj.__str__()}")
            obj = self.create_or_update_data_obj_from_csv(version, table_name, present_data_obj)
        else:
            logger.warning(
                f"Object already exists, not overwritten (overwrite = False), obj: {present_data_obj.__str__()}")
            return None

        if auto_commit:
            obj.commit(session)
            logger.info(
                f"Loading data from csv and committing to database {self.__str__()} successful (auto_commit=True)!")
        else:
            logger.info(f"Loading data from csv for {self.__str__()} successful (auto_commit=False)!")
        return obj

    # Hilfsmethode für load_data_from_csv
    def find_data_by_table_name(self, table_name: str):
        """
        Findet ein Datenobjekt anhand seines Tabellennamens.

        :param table_name: Der Name der Tabelle, die gesucht wird.
        :return: Das gefundene Datenobjekt oder None, wenn kein passendes gefunden wurde.
        """
        matching_data = [data for data in self.data if data.table_name == table_name]

        if not matching_data:
            logger.debug(f"Keine Data-Instanz mit table_name {table_name} gefunden.")
            return None

        if len(matching_data) > 1:
            logger.critical(
                f"Mehrere Data-Instanzen mit table_name {table_name} gefunden. Nur die erste Instanz wird zurückgegeben.")

        logger.debug(f"Data-Instanz mit table_name {table_name} gefunden.")
        return matching_data[0]

    # Hilfsmethode für load_data_from_csv
    def create_or_update_data_obj_from_csv(self, version: str, table_name: str,
                                           present_data_obj=None):
        """
        Erstellt oder aktualisiert ein Datenobjekt aus einer CSV-Datei.

        :param version: Die Version der Daten.
        :param table_name: Der Name der Tabelle für das Datenobjekt.
        :param present_data_obj: Ein bestehendes Datenobjekt, das aktualisiert werden soll.
        :return: Das erstellte oder aktualisierte Datenobjekt.
        """
        obj = present_data_obj or Data.load_from_csv(filepath=self.filepath, id_data=None, id_messung=self.id_messung,
                                                     version=version, table_name=table_name)
        obj.df = obj.read_csv_tms(self.filepath)
        obj.update_metadata(auto_commit=True)
        if present_data_obj:
            self.data.remove(present_data_obj)
        self.data.append(obj)
        logger.info(f"Object {obj.__str__()} successfully created/updated and attached to {self.__str__()}.")
        return obj

    @timing_decorator
    def copy(self, reset_id=False, auto_commit=False, session=None):
        new_obj = super().copy("id_messung", reset_id, auto_commit, session)
        return new_obj

    @timing_decorator
    def remove(self, auto_commit=False, session=None):
        """
        Entfernen Sie dieses Messungs-Objekt aus der Datenbank.

        :param auto_commit: Ein Flag, das angibt, ob die Änderungen automatisch übernommen werden sollen.
        :param session: Die SQL-Alchemie-Sitzung, die zum Interagieren mit der Datenbank verwendet wird.
        """
        session = db_manager.get_session(session)
        # Call the base class method to remove this Data object from the database
        result = super().remove('id_messung', auto_commit, session)
        return result

    def get_data_by_version(self, version):
        """
        Diese Methode findet eine Instanz in self.data, die die gegebene Version hat.
        Es wird ein kritischer Fehler protokolliert und None zurückgegeben, wenn mehr als eine Instanz gefunden wird.
        Es wird ein Fehler protokolliert und None zurückgegeben, wenn keine Instanz gefunden wird.
        """
        matching_versions = [data for data in self.data if data.version == version]
        if len(matching_versions) > 1:
            logger.critical(
                f"Mehrere Data-Instanzen mit version '{version}' für '{self.__str__()}' nicht verfügbar. Erste zurückgegeben.")
        if len(matching_versions) == 0:
            logger.warning(f"Keine Data-Instanzen mit version '{version}' für '{self.__str__()}' gefunden.")
            return None
        logger.debug(f"Dateninstanz {matching_versions[0].__str__()} zurückgegeben")
        return matching_versions[0]


    def load_data_by_version(self, version, session=None):
        obj = self.get_data_by_version(version)
        if obj is None:
            return None
        result = obj.load_df(session)
        return result

    @timing_decorator
    def copy_data_by_version(self, version_new=configuration.data_version_copy_default,
                             version_source=configuration.data_version_default, auto_commit=False, session=None):
        """
        Diese Methode kopiert eine Dateninstanz mit version_source zu einer neuen Instanz mit version_new.
        Es wird ein Fehler protokolliert und None zurückgegeben, wenn version_new und version_source identisch sind.
        Es wird ein kritischer Fehler protokolliert und None zurückgegeben, wenn der Tabellenname der neuen Instanz mit dem der Quellinstanz übereinstimmt.
        Es wird ein Fehler protokolliert und None zurückgegeben, wenn ein Fehler beim Kopieren der Instanz oder beim Commit auftritt.
        """
        if version_new == version_source:
            logger.error(
                f"Fehler: version_new '{version_new}' darf nicht gleich version_source '{version_source}' sein!")
            return None

        for old_obj in self.data:
            if old_obj.version == version_new:
                logger.info(
                    f"Eine Instanz von Data mit der Version '{version_new}' existiert bereits. Alte Instanz wird zurückgegeben.")
                return old_obj

        source_obj = self.get_data_by_version(version_source)
        if source_obj is None:
            logger.warning(
                f"Prozess zum Kopieren von '{self.__str__()}' von Version '{version_source}' zu Version '{version_new}' abgebrochen.")
            return None

        #  Lädt die Daten für diese Data-Instanz, falls sie noch nicht geladen wurden.
        source_obj.load_df_if_missing(session=session)

        new_obj = Data.create_new_version(source_obj, version_new)

        if new_obj is None:
            return None

        if new_obj.table_name == source_obj.table_name:
            logger.critical(
                f"Tabellenname für neue Instanz ist gleich dem Quellinstanz-Tabellennamen.")
            return None

        self.data.append(new_obj)

        if auto_commit:
            try:
                new_obj.commit(session=session)
            except Exception as e:
                logger.error(f"Fehler beim Commit der neuen Dateninstanz: {e}")
                return None

        logger.info(f"Erfolgreiche Erstellung von '{version_new.__str__()}' (auto_commit = '{auto_commit}').")

        return new_obj

    def commit_data_by_version(self, version, session=None):
        obj = self.get_data_by_version(version)
        if obj is None:
            logger.warning(f"Commit für {self.__str__()} abgebrochen.")
            return False
        #  Lädt die Daten für diese Data-Instanz, falls sie noch nicht geladen wurden.
        obj.load_df_if_missing(session=session)
        result = obj.commit()
        return result

    def limit_time_data_by_version(self, version, start_time: str, end_time: str, auto_commit: bool = False,
                                   session=None):
        obj = self.get_data_by_version(version)
        if obj is None:
            logger.warning(f"Prozess zur Zeitbegrenzung für {self.__str__()} abgebrochen.")
            return None
        #  Lädt die Daten für diese Data-Instanz, falls sie noch nicht geladen wurden.
        obj.load_df_if_missing(session=session)

        result = obj.limit_by_time(start_time, end_time, auto_commit, session)
        return result






class MessungStatus(BaseClass):
    __tablename__ = 'MessungStatus'
    id_messung_status = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    messung_status = Column(String)

# treemotion/classes/messung.py

import pandas as pd

from utilities.imports_classes import *
from utilities.path_utils import validate_and_get_filepath


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

    def __init__(self, *args, id_messung=None, id_messreihe=None, id_baum_behandlung=None, id_sensor=None,
                 id_messung_status=None, filename=None, filepath=None, id_sensor_ort=None, sensor_hoehe=None,
                 sensor_umfang=None, sensor_ausrichtung=None, **kwargs):
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
        return f"Messung(id_messung={self.id_messung}, id_messreihe={self.id_messreihe}, filename={self.filename}"

    @classmethod
    @timing_decorator
    def load_from_db(cls, id_messreihe=None, session=None):
        objs = super().load_from_db(filter_by={'id_messreihe': id_messreihe} if id_messreihe else None, session=session)
        logger.info(f"{len(objs)} Messungen wurden erfolgreich geladen.")
        return objs

    @timing_decorator
    def remove(self, id_projekt='id_messung', auto_commit=False, session=None):
        session = db_manager.get_session(session)
        # Call the base class method to remove this Data object from the database
        super().remove(id_projekt, auto_commit, session)

    @timing_decorator
    def copy(self, id_name="id_messung", reset_id=False, auto_commit=False, session=None):
        new_instance = super().copy(id_name, reset_id, auto_commit, session)
        return new_instance

    def copy_deep(self, copy_relationships=True):
        copy = super().copy_deep(copy_relationships=copy_relationships)
        return copy

    def read_csv_tms(self):
        if self.filepath is None:
            logger.warning(f"Filepath = None, Prozess abgebrochen.")
            return None

        try:
            filepath = validate_and_get_filepath(self.filepath)
        except:
            return None
        try:
            df = pd.read_csv(filepath, sep=";", parse_dates=["Time"], decimal=",")

        except pd.errors.ParserError:
            logger.error(f"Fehler beim Lesen der Datei {filepath.stem}. Überprüfen Sie das Dateiformat.")
            return None
        except Exception as e:
            logger.error(f"Ungewöhnlicher Fehler beim Laden der {filepath.stem}: {e}")
            return None

        return df

    # Hilfsmethode für load_dat_from_csv
    def find_data_by_table_name(self, table_name):
        matching_data = [data for data in self.data if data.table_name == table_name]

        if not matching_data:
            logger.debug(f"Keine Data-Instanz mit table_name {table_name} gefunden.")
            return None

        if len(matching_data) > 1:
            logger.critical(
                f"Mehrere Data-Instanzen mit table_name {table_name} gefunden. Nur die erste Instanz wird zurückgegeben.")

        logger.debug(f"Data-Instanz mit table_name {table_name} gefunden.")
        return matching_data[0]

    # Hilfsmethode für load_dat_from_csv
    @timing_decorator
    def data_obj_from_csv(self, version: str, table_name: str):
        try:
            obj = Data(id_data=None, id_messung=self.id_messung, version=version, table_name=table_name)
            logger.debug(f"Objekt erfolgreich erstellt: {obj.__str__()}")
        except Exception as e:
            logger.error(f"new_data_obj konnte nicht erstellt werden: {e}")
            return None

        try:
            obj.df = self.read_csv_tms()
        except Exception as e:
            logger.error(f"TMS-Daten konnten nicht geladen werden: {e}")
            return None

        try:
            obj.update_metadata()
        except Exception as e:
            logger.error(f"new_data_obj konnte nicht mit Data.update_metadata() aktualisiert werden: {e}")
            return None

        logger.debug(f"Objekt wurde erfolgreich erstellt: {obj}")
        return obj

    # Hilfsmethode für load_dat_from_csv
    @timing_decorator
    def new_data_obj_to_db(self, version, table_name):
        obj = None
        try:
            obj = self.data_obj_from_csv(version, table_name)
            logger.debug(f"Objekt {obj.__str__()} erfolgreich erstellt")
        except Exception as e:
            logger.error(
                f"Objekt konnte nicht mit Data.update_metadata() aktualisiert werden, obj: {obj.__str__()}, error: {e}")
            return None

        try:
            # obj.commit_to_db()
            logger.debug(f"Objekt erfolgreich erstellt und zur Datenbank hinzugefügt: {obj.__str__()}")
        except Exception as e:
            logger.error(
                f"Objekt konnte nicht mit Data.commit_to_db() zur Datenbank hinzugefügt werden, obj: {obj.__str__()}, error: {e}")
            return None
        return obj

    # Hilfsmethode für load_dat_from_csv
    @timing_decorator
    def update_data_obj_in_db(self, present_data_obj):
        try:
            # Nutzen Sie das vorhandene Datenobjekt
            obj = present_data_obj
        except Exception as e:
            logger.error(f"Vorhandenes Datenobjekt konnte nicht geladen werden: {e}")
            return None

        try:
            obj.df = self.read_csv_tms()
        except Exception as e:
            logger.error(f"TMS-Daten konnten nicht geladen werden: {e}")
            return None

        try:
            obj.update_metadata()
        except Exception as e:
            logger.error(f"Datenobjekt konnte nicht mit Data.update_metadata() aktualisiert werden: {e}")
            return None
        logger.debug(f"Vorhandenes Data-Objekt erfolgreich geladen, read_csv_tms() und update_metadata() ausgeführt: {obj.__str__()} ")
        try:
            obj.commit_to_db()
            logger.debug(f"Objekt erfolgreich aktualisiert und zur Datenbank hinzugefügt: {obj.__str__()}")
        except Exception as e:
            logger.error(
                f"Objekt konnte nicht mit Data.commit_to_db() zur Datenbank hinzugefügt werden, obj: {obj.__str__()}, error: {e}")
            return None
        return obj

    # Hilfsmethode für load_dat_from_csv
    @timing_decorator
    def load_data_from_csv(self, version=configuration.data_version_default, overwrite=False):
        if self.filepath is None:
            logger.warning(
                f"Prozess für {self.__str__()} abgebrochen - Filename fehlt in Datenbank (filename = None).")
            return None

        logger.info(f"Starte Prozess für {self.__str__()}")
        table_name = Data.new_table_name(version, self.id_messung)
        present_data_obj = self.find_data_by_table_name(table_name)

        if present_data_obj is None:
            new_data_obj = self.new_data_obj_to_db(version, table_name)
            # Update the data relationship
            self.data.append(new_data_obj)
            logger.info(f"Objekt erfolgreich NEU erstellt und zur Datenbank hinzugefügt: {new_data_obj.__str__()}")
            return new_data_obj

        elif isinstance(present_data_obj, Data):
            if overwrite:
                up_data_obj = self.update_data_obj_in_db(present_data_obj)
                # Update the data relationship
                self.data.remove(present_data_obj)
                self.data.append(up_data_obj)
                logger.info(
                    f"Objekt erstellt und altes Objekt in Datenbank erfolgreich überschrieben (overwrite = True): {up_data_obj.__str__()}")
                return up_data_obj
            if not overwrite:
                logger.warning(
                    f"Objekt bereits in Datenbank vorhanden, nicht überschrieben (overwrite = False), obj: {present_data_obj.__str__()} ")

                return present_data_obj


class MessungStatus(BaseClass):
    __tablename__ = 'MessungStatus'
    id_messung_status = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    messung_status = Column(String)

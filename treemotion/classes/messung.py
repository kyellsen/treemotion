# treemotion/classes/messung.py

from utilities.imports_classes import *
from copy import deepcopy

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
        return f"Messung(id={self.id_messung}, id_messreihe={self.id_messreihe}, filename={self.filename}"

    @classmethod
    @timing_decorator
    def load_from_db(cls, id_messreihe=None, session=None):
        objs = super().load_from_db(filter_by={'id_messreihe': id_messreihe} if id_messreihe else None, session=session)
        logger.info(f"{len(objs)} Messungen wurden erfolgreich geladen.")
        return objs

    @timing_decorator
    def remove(self, auto_commit=False, session=None):
        session = db_manager.get_session(session)
        # Call the base class method to remove this Data object from the database
        super().remove('id_messung', auto_commit, session)

    @timing_decorator
    def copy(self, reset_id=False, auto_commit=False, session=None):
        new_obj = super().copy("id_messung", reset_id, auto_commit, session)
        return new_obj

    def copy_deep(self, copy_relationships=True):
        copy = super().copy_deep(copy_relationships=copy_relationships)
        return copy

    def find_instance_by_version(self, version):
        """
        Diese Methode findet eine Instanz in self.data, die die gegebene Version hat.
        Es wird ein kritischer Fehler protokolliert und None zurückgegeben, wenn mehr als eine Instanz gefunden wird.
        Es wird ein Fehler protokolliert und None zurückgegeben, wenn keine Instanz gefunden wird.
        """
        matching_versions = [data for data in self.data if data.version == version]
        if len(matching_versions) > 1:
            logger.critical(
                f"Mehr als eine Dateninstanz mit Version {version} gefunden.")
            return None
        if len(matching_versions) == 0:
            logger.error(f"Keine Dateninstanz mit Version {version} gefunden.")
            return None
        return matching_versions[0]

    def load_data_version(self, version, session=None):
        obj = self.find_instance_by_version(version)
        result = obj.load_data(session)
        return result

    @timing_decorator
    def copy_version(self, version_new=configuration.data_version_copy_default,
                     version_source=configuration.data_version_default, auto_commit=False, session=None):
        """
        Diese Methode kopiert eine Dateninstanz mit version_source zu einer neuen Instanz mit version_new.
        Es wird ein Fehler protokolliert und None zurückgegeben, wenn version_new und version_source identisch sind.
        Es wird ein kritischer Fehler protokolliert und None zurückgegeben, wenn der Tabellenname der neuen Instanz mit dem der Quellinstanz übereinstimmt.
        Es wird ein Fehler protokolliert und None zurückgegeben, wenn ein Fehler beim Kopieren der Instanz oder beim Commit auftritt.
        """
        if version_new == version_source:
            logger.error(
                f"Fehler: version_new ({version_new}) darf nicht gleich version_source ({version_source}) sein!")
            return None

        source_obj = self.find_instance_by_version(version_source)
        if source_obj is None:
            return None

        try:
            new_obj = deepcopy(source_obj)
        except Exception as e:
            logger.error(f"Fehler beim Erstellen der Kopie der Dateninstanz: {e}")
            return None

        new_obj.id_data = None
        new_obj.version = version_new

        new_obj.table_name = new_obj.new_table_name(new_obj.version, new_obj.id_messung)

        if new_obj.table_name == source_obj.table_name:
            logger.critical(
                f"Tabellenname für neue Instanz ist gleich dem Quellinstanz-Tabellennamen.")
            return None

        self.data.append(new_obj)

        if auto_commit:
            try:
                new_obj.commit_data_obj(session=session)
            except Exception as e:
                logger.error(f"Fehler beim Commit der neuen Dateninstanz: {e}")
                return None

        logger.info(f"Erfolgreiche Erstellung von {version_new.__str__()} (auto_commit = {auto_commit}).")

        return new_obj

    @timing_decorator
    def load_data_from_csv(self, version=configuration.data_version_default, overwrite=False, auto_commit=False,
                           session=None):
        if self.filepath is None:
            logger.warning(f"Process for {self.__str__()} aborted, no filename for tms.csv (filename = None).")
            return None

        logger.info(f"Start loading csv data for {self.__str__()}")
        table_name = Data.new_table_name(version, self.id_messung)
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
            obj.commit_data_obj(session)
            logger.info(
                f"Loading data from csv and committing to database {self.__str__()} successful (auto_commit=True)!")
        else:
            logger.info(f"Loading data from csv for {self.__str__()} successful (auto_commit=False)!")
        return obj

    # Hilfsmethode für load_data_from_csv
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

    # Hilfsmethode für load_data_from_csv
    def create_or_update_data_obj_from_csv(self, version, table_name, present_data_obj=None):
        obj = present_data_obj or Data.load_from_csv(filepath=self.filepath, id_data=None, id_messung=self.id_messung,
                                                     version=version, table_name=table_name)
        obj.df = obj.read_csv_tms(self.filepath)
        obj.update_metadata()
        if present_data_obj:
            self.data.remove(present_data_obj)
        self.data.append(obj)
        logger.info(f"Object {obj.__str__()} successfully created/updated and attached to {self.__str__()}.")
        return obj


class MessungStatus(BaseClass):
    __tablename__ = 'MessungStatus'
    id_messung_status = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    messung_status = Column(String)

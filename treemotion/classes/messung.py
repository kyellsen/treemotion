# treemotion/classes/messung.py
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
import pandas as pd

from .data import Data
from utilities.base import Base
from utilities.timing import timing_decorator

from utilities.log import get_logger
logger = get_logger(__name__)

class Messung(Base):
    __tablename__ = 'Messung'
    id_messung = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    id_messreihe = Column(Integer, ForeignKey('Messreihe.id_messreihe'))
    id_baum_behandlung = Column(Integer, ForeignKey('BaumBehandlung.id_baum_behandlung'))
    id_sensor = Column(Integer, ForeignKey('Sensor.id_sensor'))
    id_messung_status = Column(Integer, ForeignKey('MessungStatus.id_messung_status'))
    filename = Column(String)
    filepath = Column(String)
    id_sensor_ort = Column(Integer, ForeignKey('SensorOrt.id_sensor_ort'))
    sensor_hoehe = Column(Integer)
    sensor_umfang = Column(Integer)
    sensor_ausrichtung = Column(Integer)

    # Verweis auf Data-Instanzen
    data = relationship("Data", cascade="all, delete-orphan")

    def __init__(self, session, id_messung=None, id_messreihe=None, id_baum_behandlung=None, id_sensor=None,
                 id_messung_status=None, filename=None, filepath=None, id_sensor_ort=None, sensor_hoehe=None,
                 sensor_umfang=None, sensor_ausrichtung=None):
        # Speichern des Sessions-Objektes (Standard aus Messreihe-Klasse)
        self.session = session

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
        # additional only in class-object
        self.data_list = []

    @classmethod
    def from_database(cls, db_messung, session):
        obj = cls(session)
        obj.id_messung = db_messung.id_messung
        obj.id_messreihe = db_messung.id_messreihe
        obj.id_baum_behandlung = db_messung.id_baum_behandlung
        obj.id_sensor = db_messung.id_sensor
        obj.id_messung_status = db_messung.id_messung_status
        obj.filename = db_messung.filename
        obj.filepath = db_messung.filepath
        obj.id_sensor_ort = db_messung.id_sensor_ort
        obj.sensor_hoehe = db_messung.sensor_hoehe
        obj.sensor_umfang = db_messung.sensor_umfang
        obj.sensor_ausrichtung = db_messung.sensor_ausrichtung
        return obj

    def is_version_in_data_list(self, version):
        return any(data.version == version for data in self.data_list)

    def is_version_in_db(self, version):
        pass
        ### Hier code einfügen

    @timing_decorator
    def add_data_from_csv(self, version="raw"):
        if self.is_version_in_data_list(version):
            logger.info(f"Version {version} bereits vorhanden in Instanz, Daten werden nicht erneut hinzugefügt.")
            return

        table_name = Messung.get_table_name(id_messung=self.id_messung, version=version)

        existing_data = self.session.query(Data).filter_by(table_name=table_name).first()
        if existing_data:
            data_id = existing_data.id_data
        else:
            data_id = None

        obj = Data(id_data=data_id, id_messung=self.id_messung, version=version)
        obj.data = self.read_csv(filepath=self.filepath)
        obj.update_metadata()
        obj.table_name = table_name
        obj.to_database(self.session)
        self.data_list.append(obj)
        logger.info(f"add_data_from_csv '{self.filename}', table_name '{table_name}', id_data '{data_id}'.")

    @timing_decorator
    def add_data_from_db(self, version):
        if self.is_version_in_data_list(version):
            logger.info(f"Version {version} bereits vorhanden in Instanz, Daten werden nicht erneut hinzugefügt.")
            return

        db_data_list = self.session.query(Data).filter_by(id_messung=self.id_messung).all()

        for db_data in db_data_list:
            if any(data.id_data == db_data.id_data for data in self.data_list) or db_data.version != version:
                continue

            data = Data.from_database(db_data, self.session)
            self.data_list.append(data)
            logger.info(f"add_data_from_db '{data.table_name}', id_data '{data.id_data}'.")

    @timing_decorator
    def delete_data_from_db(self, version):
        if not self.is_version_in_data_list(version):
            logger.warning(f"Version {version} nicht vorhanden, keine Daten zum Löschen gefunden.")
            return

        try:
            # Lösche alle Data-Objekte, die der angegebenen Version entsprechen
            self.session.query(Data).filter_by(id_messung=self.id_messung, version=version).delete()
            self.session.commit()

            # Entferne die gelöschten Data-Objekte aus der data_list
            self.data_list = [data for data in self.data_list if data.version != version]
            logger.info(f"Daten der Version {version} erfolgreich aus der Datenbank gelöscht.")
        except Exception as e:
            logger.error(f"Ein Fehler ist beim Löschen der Daten der Version {version} aufgetreten: {e}", "error")

    @staticmethod
    def get_table_name(id_messung: int, version: str):
        return f"auto_data_{version}_id_messung_{id_messung}"

    @staticmethod
    @timing_decorator
    def read_csv(filepath):
        data = pd.read_csv(filepath, sep=";", parse_dates=["Time"], decimal=",")
        return data

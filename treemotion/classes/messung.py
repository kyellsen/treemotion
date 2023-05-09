from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
import pandas as pd

from utilities.base import Base
from utilities.timing import timing_decorator
from .data import Data


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

    @timing_decorator
    def add_data_from_csv(self, version="raw"):
        table_name = Messung.get_table_name(id_messung=self.id_messung, version=version)

        # Prüfen, ob bereits eine Zeile mit dem gleichen table_name vorhanden ist
        existing_data = self.session.query(Data).filter_by(table_name=table_name).first()
        if existing_data:
            data_id = existing_data.id_data
        else:
            data_id = None

        # Erstellen einer neuen Data-Instanz
        obj = Data(id_data=data_id, id_messung=self.id_messung, version=version)
        obj.data = self.read_csv(filepath=self.filepath)
        obj.update_metadata()
        obj.table_name = table_name
        obj.to_database(self.session)
        self.data_list.append(obj)
        print(
            f"Messung {self.id_messung}: add_data_from_csv '{self.filename}', table_name '{table_name}', id_data '{data_id}'.")

    @timing_decorator
    def add_data_from_db(self, version):
        db_data_list = self.session.query(Data).filter_by(id_messung=self.id_messung).all()

        for db_data in db_data_list:
            # Überprüfen, ob die Messung bereits in der Liste ist und ob die Version der Daten übereinstimmt
            if any(data.id_data == db_data.id_data for data in self.data_list) or db_data.version != version:
                continue

            # Überprüfen, ob die angegebene Version bereits in der data_list der Messung vorhanden ist
            if any(data.version == version for data in self.data_list):
                print(
                    f"Messung {self.id_messung}: Version {version} bereits vorhanden, Daten werden nicht erneut hinzugefügt.")
                continue

            data = Data.from_database(db_data, self.session)
            self.data_list.append(data)
            print(
                f"Messung {self.id_messung}: add_data_from_db '{data.table_name}', id_data '{data.id_data}'.")

    @timing_decorator
    def delete_data_from_db(self, version):
        # Lösche alle Data-Objekte, die der angegebenen Version entsprechen
        self.session.query(Data).filter_by(id_messung=self.id_messung, version=version).delete()
        self.session.commit()

        # Entferne die gelöschten Data-Objekte aus der data_list
        self.data_list = [data for data in self.data_list if data.version != version]


    @staticmethod
    @timing_decorator
    def read_csv(filepath):
        data = pd.read_csv(filepath, sep=";", parse_dates=["Time"], decimal=",")
        return data

    @staticmethod
    def get_table_name(id_messung: int, version: str):
        return f"auto_data_{version}_id_messung_{id_messung}"

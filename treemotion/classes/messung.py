from sqlalchemy import Column, Integer, String, ForeignKey

from utilities.base import Base
import os
import glob



class Messung(Base):
    __tablename__ = 'Messung'
    id_messung = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    id_messreihe = Column(Integer, ForeignKey('Messreihe.id_messreihe'))
    id_baum_behandlung = Column(Integer, ForeignKey('BaumBehandlung.id_baum_behandlung'))
    id_sensor = Column(Integer, ForeignKey('Sensor.id_sensor'))
    id_messung_status = Column(Integer, ForeignKey('MessungStatus.id_messung_status'))
    filename = Column(String)
    id_sensor_ort = Column(Integer, ForeignKey('SensorOrt.id_sensor_ort'))
    sensor_hoehe = Column(Integer)
    sensor_umfang = Column(Integer)
    sensor_ausrichtung = Column(Integer)

    def __init__(self, id_messung=None, id_messreihe=None, id_baum_behandlung=None, id_sensor=None,
                 id_messung_status=None, filename=None, filepath=None, id_sensor_ort=None, sensor_hoehe=None,
                 sensor_umfang=None, sensor_ausrichtung=None):
        # in SQLite Database
        self.id_messung = id_messung
        self.id_messreihe = id_messreihe
        self.id_baum_behandlung = id_baum_behandlung
        self.id_sensor = id_sensor
        self.id_messung_status = id_messung_status
        self.filename = filename
        self.id_sensor_ort = id_sensor_ort
        self.sensor_hoehe = sensor_hoehe
        self.sensor_umfang = sensor_umfang
        self.sensor_ausrichtung = sensor_ausrichtung
        # additional only in class-object
        self.filepath = filepath

    @classmethod
    def from_database(cls, db_messung):
        obj = cls()
        obj.id_messung = db_messung.id_messung
        obj.id_messreihe = db_messung.id_messreihe
        obj.id_baum_behandlung = db_messung.id_baum_behandlung
        obj.id_sensor = db_messung.id_sensor
        obj.id_messung_status = db_messung.id_messung_status
        obj.filename = db_messung.filename
        obj.id_sensor_ort = db_messung.id_sensor_ort
        obj.sensor_hoehe = db_messung.sensor_hoehe
        obj.sensor_umfang = db_messung.sensor_umfang
        obj.sensor_ausrichtung = db_messung.sensor_ausrichtung
        return obj


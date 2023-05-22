from utils.imports_classes import *

logger = get_logger(__name__)


class Sensor(BaseClass):
    __tablename__ = 'Sensor'
    id_sensor = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    id_sensor_typ = Column(Integer, ForeignKey('SensorTyp.id_sensor_typ', onupdate='CASCADE'))
    id_sensor_status = Column(Integer, ForeignKey('SensorStatus.id_sensor_status', onupdate='CASCADE'))
    anmerkung = Column(String)

    sensor_typ = relationship('SensorTyp', lazy="joined")
    sensor_status = relationship('SensorStatus', lazy="joined")


class SensorStatus(BaseClass):
    __tablename__ = 'SensorStatus'
    id_sensor_status = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    sensor_status = Column(String)


class SensorTyp(BaseClass):
    __tablename__ = 'SensorTyp'
    id_sensor_typ = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    sensor_typ = Column(String)


class SensorOrt(BaseClass):
    __tablename__ = 'SensorOrt'
    id_sensor_ort = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    sensor_ort = Column(String)

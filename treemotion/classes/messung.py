# treemotion/classes/messung.py

import pandas as pd

from utilities.common_imports import *

from .data import Data


class Messung(BaseClass):
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
    data_list = relationship("Data", backref="Messung", lazy='select', cascade="all, delete, delete-orphan")

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

    @classmethod
    @timing_decorator
    def load_from_db(cls, path_db, id_messreihe=None, load_related=configuration.messung_load_related_default):
        objs = super().load_from_db(path_db, filter_by={'id_messreihe': id_messreihe} if id_messreihe else None,
                                    load_related=load_related, related_attribute=cls.data_list)
        logger.info(f"{len(objs)} Messungen wurden erfolgreich geladen.")
        return objs
    @timing_decorator
    def add_to_db(self, *args, path_db, update=False):
        super().add_to_db(path_db, id_name='id_messung', update=update)

    @timing_decorator
    def remove_from_db(self, *args, path_db):
        # Call the base class method to remove this Data object from the database
        super().remove_from_db(path_db, id_name='id_messung')




    # def is_version_in_data_list(self, version):
    #     """
    #     Checks if a specific version is present in the data list.
    #
    #     Args:
    #         version (str): Version to be checked.
    #
    #     Returns:
    #         bool: True if the version is in the data list, False otherwise.
    #     """
    #     return any(data.version == version for data in self.data_list)
    #
    # def is_version_in_db(self, version):
    #     """
    #     Checks if a specific version is present in the database.
    #
    #     Args:
    #         version (str): Version to be checked.
    #
    #     Returns:
    #         bool: True if the version is in the database, False otherwise.
    #     """
    #     existing_data = self.session.query(Data).filter_by(id_messung=self.id_messung, version=version).first()
    #     return existing_data is not None
    #
    # @staticmethod
    # @timing_decorator
    # def read_csv(filepath):
    #     data = pd.read_csv(filepath, sep=";", parse_dates=["Time"], decimal=",")
    #     return data
    #
    # @timing_decorator
    # def add_data_from_csv(self, version=configuration.data_version_default):
    #     version_in_data_list = self.is_version_in_data_list(version)
    #     version_in_db = self.is_version_in_db(version)
    #
    #     if not version_in_data_list and not version_in_db:
    #
    #         table_name = Messung.get_table_name(id_data=self.id_data, id_messung=self.id_messung, version=version)
    #
    #         logger.info(f"Version {version} wurde Datenbank und Instanz neu hinzugefügt als {table_name}")
    #     elif version_in_data_list and version_in_db:
    #         logger.info(
    #             f"Version {version} bereits in Instanz und Datenbank vorhanden, Daten werden nicht erneut hinzugefügt.")
    #         return
    #
    #     elif version_in_data_list and not version_in_db:
    #         # Get the data from data_list where version = version
    #         data_for_db = next(data for data in self.data_list if data.version == version)
    #         # Save the data to the database
    #         data_for_db.add_to_db(self.session)
    #         logger.info(f"Version {version} bereits in Instanz vorhanden, Daten werden zur Datenbank hinzugefügt.")
    #         return
    #
    #     elif version_in_db and not version_in_data_list:
    #         # Get the data from the database where version = version
    #         data_for_list = self.session.query(Data).filter_by(id_messung=self.id_messung, version=version).first()
    #         # Add the data to data_list
    #         self.data_list.append(data_for_list)
    #         logger.info(f"Version {version} bereits in der Datenbank vorhanden, Daten werden zur Instanz hinzugefügt.")
    #         return
    #
    #     existing_data = self.session.query(Data).filter_by(table_name=table_name).first()
    #     if existing_data:
    #         data_id = existing_data.id_data
    #     else:
    #         data_id = None
    #
    #     obj = Data(id_data=data_id, id_messung=self.id_messung, version=version)
    #     obj.data = self.read_csv(filepath=self.filepath)
    #     obj.update_metadata()
    #     obj.table_name = table_name
    #     obj.add_to_db(self.session)
    #     self.data_list.append(obj)
    #     logger.info(f"add_data_from_csv '{self.filename}', table_name '{table_name}', id_data '{data_id}'.")

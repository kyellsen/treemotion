from kj_core.classes.core_data_class import CoreDataClass

from ..common_imports.imports_classes import *
from .data_tms import DataTMS
from .data_wind_station import DataWindStation

logger = get_logger(__name__)


class DataMerge(CoreDataClass, BaseClass):
    __tablename__ = 'DataMerge'
    data_id = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    data_filepath = Column(String, unique=True)
    datetime_added = Column(DateTime)
    datetime_last_edit = Column(DateTime)
    measurement_version_id = Column(Integer,
                                    ForeignKey('MeasurementVersion.measurement_version_id', onupdate='CASCADE'),
                                    nullable=False)
    data_tms_id = Column(Integer, ForeignKey('DataTMS.data_id', onupdate='CASCADE'), nullable=False)
    data_wind_station_id = Column(Integer,
                                  ForeignKey('DataWindStation.data_id', onupdate='CASCADE'), nullable=False)

    data_tms = relationship("DataTMS", backref="data_merge", )
    data_wind_station = relationship("DataWindStation", backref="data_merge")

    datetime_column_name = "datetime"

    def __init__(self, data_id: int = None, data: pd.DataFrame = None, data_filepath: str = None, datetime_added=None,
                 datetime_last_edit=None,
                 measurement_version_id: int = None, data_tms_id: int = None, data_wind_station_id: int = None):
        CoreDataClass.__init__(self, data_id=data_id, data=data, data_filepath=data_filepath,
                               datetime_added=datetime_added, datetime_last_edit=datetime_last_edit)

        self.measurement_version_id = measurement_version_id
        self.data_tms_id = data_tms_id
        self.data_wind_station_id = data_wind_station_id

    def __str__(self):
        """
        Returns a string representation of the DataWind instance.
        """
        return f"{self.__class__.__name__}: data_id='{self.data_id}', tms={self.data_tms}, station='{self.data_wind_station}'"

    @classmethod
    def create_from_measurement_version(cls, measurement_version, data_tms: 'DataTMS', data_wind_station: 'DataWindStation') -> 'DataMerge':
        try:
            # create data_filepath
            data_directory: Path = cls.get_config().data_directory
            folder: Path = cls.get_config().DataMerge.data_directory
            filename: str = cls.get_data_manager().get_new_filename(
                data_id=measurement_version.measurement_version_id,
                prefix=f"data_merge",
                file_extension="feather"
            )
            data_filepath = str(data_directory / folder / filename)

            # create data
            #df_merged = cls.get_merged_data(data_wind_station, data_tms)

            obj = cls(data=df_merged, data_filepath=data_filepath, measurement_version_id=measurement_version.measurement_version_id, data_wind_station_id=data_wind_station.data_id)
            logger.debug(f"New {obj}")
            session = cls.get_database_manager().session
            session.add(obj)

            return obj
        except Exception as e:
            logger.error(
                f"Error creating object from station from '{measurement_version}', '{data_wind_station}' and '{data_tms}': {e}")
            raise

    def update_from_station(self, measurement_version) -> None:
        """
        Updates the object with data from the station.

        Raises:
            ValueError: Raised if data_wind_station is not set.
            Exception: Propagates any exceptions that occur during the update.
        """
        try:
            if not self.data_wind_station:
                raise ValueError("Data wind station is not set.")

            self.data = self.get_data_from_station(measurement_version, self.data_wind_station)

        except ValueError as e:
            logger.error(f"Validation error in update_from_station: {e}")
            raise
        except Exception as e:
            logger.error(f"Error in updating from station: {e}")
            raise

    def get_merged_data(self):
        pass



    @classmethod
    def get_data_from_station(cls, measurement_version, data_wind_station) -> pd.DataFrame:
        """
        Retrieves data from the station and stores it in the object.

        Raises:
            ValueError: If the data_wind_station or measurement_version is missing, or if the data is not a DataFrame.
            Exception: Propagates any exceptions that occur during data retrieval.
        """
        try:
            if not measurement_version or not data_wind_station:
                raise ValueError("MeasurementVersion or DataWindStation are missing.")

            datetime_start = measurement_version.data_tms.datetime_start
            datetime_end = measurement_version.data_tms.datetime_end
            time_col = cls.datetime_column_name

            if time_col not in data_wind_station.data.columns:
                raise ValueError(f"Time column '{time_col}' not found in data.")

            data = data_wind_station.data.copy()
            data = data.loc[(data[time_col] >= datetime_start) & (data[time_col] <= datetime_end)]
            data.reset_index(drop=True, inplace=True)

            if not isinstance(data, pd.DataFrame):
                raise ValueError(f"{cls.__name__}.data is not a pandas DataFrame, it is: '{type(data)}'")
            logger.info(f"Data from station retrieved and set for '{cls}'")

            return data

        except ValueError as e:
            logger.error(f"Validation error in get_data_from_station: {e}")
            raise
        except Exception as e:
            logger.error(f"Error in getting data from station: {e}")
            raise

    def test(self):
        pass

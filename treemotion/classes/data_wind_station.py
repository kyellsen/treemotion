from kj_core.classes.core_data_class import CoreDataClass

from ..common_imports.imports_classes import *

from ..wind.wind_dwd_download import download_dwd_files
from ..wind.wind_file_extraction import load_wind_df, load_station_metadata

logger = get_logger(__name__)


class DataWindStation(CoreDataClass, BaseClass):
    __tablename__ = 'DataWindStation'
    data_id = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    data_filepath = Column(String, unique=True)
    datetime_added = Column(DateTime)
    datetime_last_edit = Column(DateTime)
    station_name = Column(String)
    station_id = Column(String, nullable=False, unique=True)
    bundesland = Column(String)
    station_height = Column(Integer)
    station_latitude = Column(Float)
    station_longitude = Column(Float)
    source = Column(String)

    def __init__(self, data_id=None, data=None, data_filepath=None, datetime_added=None, datetime_last_edit=None,
                 station_id=None, station_name=None, bundesland=None,
                 station_height=None, station_latitude=None, station_longitude=None, source=None):
        CoreDataClass.__init__(self, data_id=data_id, data=data, data_filepath=data_filepath,
                               datetime_added=datetime_added, datetime_last_edit=datetime_last_edit)

        self.station_id = station_id
        self.station_name = station_name
        self.bundesland = bundesland
        self.station_height = station_height
        self.station_latitude = station_latitude
        self.station_longitude = station_longitude
        self.source = source

    def __str__(self):
        """
        Returns a string representation of the DataWindStation object.
        """
        return f"{self.__class__.__name__}(station_id={self.station_id}, station_name={self.station_name})"

    @classmethod
    def create_from_dwd(cls,
                        station_id: str,
                        alternative_filename_wind: Optional[str] = None,
                        alternative_filename_wind_extreme: Optional[str] = None,
                        alternative_filename_stations_list: Optional[str] = None):
        data_directory = cls.get_data_manager().data_directory
        folder = cls.get_config().DataWindStation.data_directory
        filename = (cls.
                    get_data_manager().
                    get_new_filename(station_id,
                                     prefix="wind",
                                     file_extension="feather"))
        filepath = data_directory / folder / filename
        obj = cls(station_id=station_id, data_filepath=str(filepath))

        data_directory = cls.get_data_manager().data_directory
        folder = cls.get_config().DataWindStation.download_folder
        path = data_directory / folder / station_id

        try:
            filename_wind, filename_wind_extreme, filename_stations_list = download_dwd_files(station_id, path)
        except Exception as e:
            logger.error(f"Error downloading files: {e}")
            return None

        # Use alternative filenames provided as arguments
        if alternative_filename_wind:
            filename_wind = alternative_filename_wind
        if alternative_filename_wind_extreme:
            filename_wind_extreme = alternative_filename_wind_extreme
        if alternative_filename_stations_list:
            filename_stations_list = alternative_filename_stations_list

        obj.read_dwd_files(filename_wind, filename_wind_extreme, filename_stations_list)
        return obj

    def read_dwd_files(self, filename_wind, filename_wind_extreme, filename_stations_list):
        """
        Loads wind measurement data from online DWD (Deutscher Wetterdienst) resources.
        """
        data_directory = self.get_data_manager().data_directory
        folder = self.get_config().DataWindStation.download_folder
        path = data_directory / folder / self.station_id

        # Check if the downloaded files actually exist
        for filename in [filename_wind, filename_wind_extreme, filename_stations_list]:
            full_path = path / filename if filename else None
            if full_path and not full_path.exists():
                logger.error(f"File {full_path} was expected but is not present in the directory {path}.")
                return None

        try:
            wind_df = load_wind_df(path.joinpath(filename_wind), path.joinpath(filename_wind_extreme))
        except Exception as e:
            logger.error(f"Error while loading and preparing dataframes: {e}")
            return None

        try:
            station_metadata = load_station_metadata(self.station_id, path.joinpath(filename_stations_list))
        except Exception as e:
            logger.error(
                f"Error while loading and preparing station metadata from {filename_stations_list}: {e}")
            return None

        self.data = wind_df

        self.station_name = station_metadata['station_name']
        self.bundesland = station_metadata['bundesland']
        self.station_height = station_metadata['station_height']
        self.station_latitude = station_metadata['station_latitude']
        self.station_longitude = station_metadata['station_longitude']
        self.source = None
        return

    @property
    def datetime_start(self):
        datetime_column_name = self.get_config().DataWindStation.datetime_column_name
        if self.data is not None and datetime_column_name in self.data.columns:
            return self.data[datetime_column_name].min()
        return None

    @property
    def datetime_end(self):
        datetime_column_name = self.get_config().DataWindStation.datetime_column_name
        if self.data is not None and datetime_column_name in self.data.columns:
            return self.data[datetime_column_name].max()
        return None

    @property
    def duration(self):
        if self.datetime_start and self.datetime_end:
            return (self.datetime_end - self.datetime_start).total_seconds()
        return None

    @property
    def length(self):
        if self.data is not None:
            return len(self.data)
        return None

from kj_core.classes.core_data_class import CoreDataClass

from ..common_imports.imports_classes import *

from ..wind.wind_dwd_download import download_wind_file, download_wind_extreme_file, download_station_list_file
from ..wind.wind_file_extraction import extract_wind_df, extract_station_metadata
from kj_core.utils.df_utils import validate_df

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
    def create_from_station(cls, station_id: str, filename_wind: Optional[str] = None,
                            filename_wind_extreme: Optional[str] = None,
                            filename_stations_list: Optional[str] = None) -> Optional['DataWindStation']:
        obj = cls(station_id=station_id, data_filepath=str(cls._get_feather_file_path(station_id)))
        if obj.get_dwd_data(station_id, filename_wind,
                            filename_wind_extreme, filename_stations_list):
            return obj
        return None

    # helper method
    @classmethod
    def _get_feather_file_path(cls, station_id: str) -> Path:
        data_directory = cls.get_data_manager().data_directory
        folder = cls.get_config().DataWindStation.data_directory
        filename = cls.get_data_manager().get_new_filename(station_id, prefix="wind", file_extension="feather")
        return data_directory / folder / filename

    def update_from_dwd(self, filename_wind: Optional[str] = None,
                        filename_wind_extreme: Optional[str] = None,
                        filename_stations_list: Optional[str] = None) -> bool:

        self.get_dwd_data(self.station_id, filename_wind,
                          filename_wind_extreme, filename_stations_list)
        return self

    def get_dwd_data(self, station_id: str, filename_wind: Optional[str],
                     filename_wind_extreme: Optional[str],
                     filename_stations_list: Optional[str]) -> bool:
        path = self._get_download_folder_path(station_id)

        # Use alternative filenames provided as arguments
        if not filename_wind:
            filename_wind = download_wind_file(station_id, path)

        if not filename_wind_extreme:
            filename_wind_extreme = download_wind_extreme_file(station_id, path)

        if not filename_stations_list:
            filename_stations_list = download_station_list_file(path)

        self.read_dwd_files(filename_wind, filename_wind_extreme, filename_stations_list)
        return self

    def _get_download_folder_path(self, station_id: str) -> Path:
        data_directory = self.get_data_manager().data_directory
        folder = self.get_config().DataWindStation.download_folder
        return data_directory / folder / station_id

    def read_dwd_files(self, filename_wind, filename_wind_extreme, filename_stations_list):
        """
        Loads wind measurement data from online DWD (Deutscher Wetterdienst) resources.
        """
        path = self._get_download_folder_path(self.station_id)

        # Check if the downloaded files actually exist
        for filename in [filename_wind, filename_wind_extreme, filename_stations_list]:
            full_path = path / filename if filename else None
            if full_path and not full_path.exists():
                logger.error(f"File {full_path} was expected but is not present in the directory {path}.")
                return None

        try:
            wind_df = extract_wind_df(path.joinpath(filename_wind), path.joinpath(filename_wind_extreme))
        except Exception as e:
            logger.error(f"Error while loading and preparing dataframes: {e}")
            return None

        try:
            station_metadata = extract_station_metadata(self.station_id, str(path.joinpath(filename_stations_list)))
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

    def validate_data(self) -> bool:
        """
        Checks if the DataFrame data is valid and contains the required columns.

        Returns:
            bool: True if the DataFrame is valid, False otherwise.
        """
        try:
            validate_df(df=self.data, columns=self.get_config().DataWindStation.data_columns)
            logger.debug(f"Data validation for '{self}' correct!")
            return True
        except Exception as e:
            logger.error(f"Error during validation of the DataFrame: {e}")
            return False

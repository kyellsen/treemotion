# treemotion/classes/wind_measurement.py

from sqlalchemy import func

from utils.imports_classes import *
from wind.wind_dwd_download import download_dwd_files
from utils.path_utils import validate_files_exist

from .wind_data import WindData

logger = get_logger(__name__)


class WindMeasurement(BaseClass):
    """
    This class represents a wind measurement series in the system.
    """
    __tablename__ = 'WindMeasurement'
    wind_measurement_id = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    wind_measurement_name = Column(String)
    station_id = Column(Integer)
    station_name = Column(String)
    bundesland = Column(String)
    station_height = Column(Integer)
    station_latitude = Column(Float)
    station_longitude = Column(Float)
    source = Column(String)
    datetime_added = Column(DateTime)
    datetime_last_edit = Column(DateTime)  # metadata
    datetime_start = Column(DateTime)  # metadata
    datetime_end = Column(DateTime)  # metadata
    duration = Column(Float)  # metadata
    length = Column(Integer)  # metadata

    wind_data = relationship(WindData, backref='wind_measurement', lazy="joined",
                             cascade="all, delete, delete-orphan", order_by=WindData.wind_data_id)

    def __init__(self, wind_measurement_id: int = None, name: str = None, source: str = None):
        """
        Constructor for the WindMeasurement class.
        """
        self.wind_measurement_id = wind_measurement_id
        self.wind_measurement_name = name
        self.station_id = None
        self.station_name = None
        self.bundesland = None
        self.station_height = None
        self.station_latitude = None
        self.station_longitude = None
        self.source = source
        self.datetime_added = func.datetime('now')
        self.datetime_last_edit = func.datetime('now')
        self.datetime_start = None
        self.datetime_end = None
        self.duration = None
        self.length = None

    def __str__(self):
        """
        Returns a string representation of the WindMeasurement instance.
        """
        return f"WindMeasurement(id={self.wind_measurement_id}, name={self.wind_measurement_name})"

    def get_wind_df(self, datetime_start=None, datetime_end=None, time_col: str = 'datetime', columns=None,
                    session=None):
        """
        Extracts wind data for the current instance from the WindData table as a Pandas DataFrame.
        It filters the data by a specified time period and includes only specified columns if specified.

        Parameters
        ----------
        datetime_start : datetime-like, optional
            The start of the time period to extract data for.
            If None, extraction is not bounded by start time.
        datetime_end : datetime-like, optional
            The end of the time period to extract data for.
            If None, extraction is not bounded by end time.
        time_col : str, optional
            The name of the column to use for time filtering. Default is 'datetime'.
        columns : list of str, optional
            The columns to include in the output. If None, all columns are included.
        session : sqlalchemy.orm.Session, optional
            The session to use for the query. If None, a new session is created.

        Returns
        -------
        pandas.DataFrame
            A DataFrame containing the extracted data.

        Raises
        ------
        AttributeError
            If 'time_col' or any column in 'columns' do not exist in WindData.
        """
        try:
            session = db_manager.get_session(session)

            # Verify that the time_col exists
            if not hasattr(WindData, time_col):
                logger.error(f"Invalid column name for time_col: {time_col}")
                raise AttributeError(f"Invalid column name for time_col: {time_col}")

            if columns is not None:
                # Verify that all specified columns exist
                for col in columns:
                    if not hasattr(WindData, col):
                        logger.error(f"Invalid column name in columns: {col}")
                        raise AttributeError(f"Invalid column name in columns: {col}")
                query = session.query(*[getattr(WindData, col) for col in columns]).filter(
                    WindData.wind_measurement_id == self.wind_measurement_id)
            else:
                query = session.query(WindData).filter(WindData.wind_measurement_id == self.wind_measurement_id)

            if datetime_start is not None:
                query = query.filter(getattr(WindData, time_col) >= datetime_start)
            if datetime_end is not None:
                query = query.filter(getattr(WindData, time_col) <= datetime_end)

            df = pd.read_sql(query.statement, session.bind)

            logger.info(
                f"Executing query for WindMeasurement with id {self.wind_measurement_id} from {datetime_start} to {datetime_end}")
            return df
        except Exception as e:
            logger.error(
                f"Error while getting wind data for WindMeasurement with id {self.wind_measurement_id}: {str(e)}")
            raise e

    def update_metadata(self):
        """
        Updates the metadata of the WindMeasurement based on the associated WindData instances.
        """
        try:
            logger.info("Start updating metadata for WindMeasurement.")
            # Filter the measurements by the correct WindMeasurement
            measurements = self.wind_data
            self.datetime_start = min([m.datetime for m in measurements])
            logger.debug(f"Start datetime of the time series: {self.datetime_start}")
            self.datetime_end = max([m.datetime for m in measurements])
            logger.debug(f"End datetime of the time series: {self.datetime_end}")
            self.duration = (self.datetime_end - self.datetime_start).total_seconds()
            logger.debug(f"Duration of the time series: {self.duration} seconds")
            self.length = len(measurements)
            logger.debug(f"Number of rows in the time series: {self.length}")

            self.datetime_last_edit = func.datetime('now')
            logger.info("Metadata for WindMeasurement successfully updated.")

        except Exception as e:
            logger.error(f"Error while updating the metadata of the WindMeasurement: {str(e)}")
            return False
        return True

    @classmethod
    def load_from_dwd_online(cls, name, station_id, folder_path: str = None,
                             link_wind="https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/wind/recent/",
                             link_wind_extreme="https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/extreme_wind/recent/",
                             link_stations_list="https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/wind/recent/zehn_min_ff_Beschreibung_Stationen.txt",
                             overwrite=False, auto_commit=False, session=None):
        """
        Loads wind measurement data from online DWD (Deutscher Wetterdienst) resources.

        :param name: Name of the WindMeasurement.
        :param station_id: Identifier for the station to load data from.
        :param folder_path: The folder path to store downloaded files.
        :param link_wind: Link to online resource for average wind data.
        :param link_wind_extreme: Link to online resource for extreme wind data.
        :param link_stations_list: Link to online resource for stations list.
        :param overwrite: If True, existing data will be overwritten. Defaults to False.
        :param auto_commit: If True, the database session will commit automatically. Defaults to False.
        :param session: Database session, if one is already open. Defaults to None.
        :return: An instance of WindMeasurement class filled with wind measurement data.
        """
        logger.info("Start loading data from online DWD resources.")

        if folder_path is None:
            folder_path = Path(config.working_directory) / config.dwd_data_directory

        folder_path, filename_wind, filename_wind_extreme, filename_stations_list = download_dwd_files(station_id,
                                                                                                       folder_path,
                                                                                                       link_wind,
                                                                                                       link_wind_extreme,
                                                                                                       link_stations_list)

        obj = cls.load_from_dwd_txt_file(name, station_id, folder_path, filename_wind, filename_wind_extreme,
                                         filename_stations_list, overwrite, auto_commit, session)
        logger.info("Data successfully loaded from online DWD resources.")
        return obj

    @classmethod
    def load_from_dwd_txt_file(cls,
                               name: str,
                               station_id: int,
                               folder_path: Union[str, Path],
                               filename_wind: str,
                               filename_wind_extreme: str,
                               filename_stations_list: str,
                               overwrite: bool = False,
                               auto_commit: bool = False,
                               session=None):
        """
        Load wind measurement data from local txt files downloaded from the German Weather Service (DWD).

        :param name: Name of the WindMeasurement.
        :param station_id: Identifier of the station from which the data originates.
        :param folder_path: Path to the folder where the txt files are located.
        :param filename_wind: Name of the txt file containing the wind data.
        :param filename_wind_extreme: Name of the txt file containing the extreme wind data.
        :param filename_stations_list: Name of the txt file containing the list of stations.
        :param overwrite: If True, existing data will be overwritten. Defaults to False.
        :param auto_commit: If True, the database session will commit automatically. Defaults to False.
        :param session: Database session, if one is already open. Defaults to None.
        :return: An instance of WindMeasurement class filled with wind measurement data, or None if an error occurred.
        """
        logger.info("Start loading DWD data from pre-downloaded files.")
        folder_path = Path(folder_path)

        files = [filename_wind, filename_wind_extreme, filename_stations_list]

        # Check if files exist
        for file in files:
            if not validate_files_exist(folder_path, file):
                return None

        try:
            merged_df = cls.load_and_prepare_dataframes(folder_path, filename_wind, filename_wind_extreme)
        except Exception as e:
            logger.error("Error while loading and preparing dataframes: " + str(e))
            return None

        # Load station metadata
        try:
            station_metadata = cls.load_and_prepare_station_metadata(station_id, folder_path, filename_stations_list)
        except Exception as e:
            logger.error(
                f"Error while loading and preparing station metadata from {filename_stations_list}: " + str(e))
            return None

        wind_measurement = cls._create_wind_measurement(name, merged_df, station_metadata)
        if wind_measurement is None:
            return None

        return cls._handle_db_session(session, wind_measurement, auto_commit, overwrite)

    @staticmethod
    def load_and_prepare_dataframes(folder_path: Path, filename_wind: str, filename_wind_extreme: str):
        """
        Loads and prepares the dataframes from the provided txt files.

        :param folder_path: The Path where the txt files are located.
        :param filename_wind: Name of the first txt file.
        :param filename_wind_extreme: Name of the second txt file.
        :return: Merged DataFrame with prepared data.
        """
        # Read each file into a DataFrame
        df1 = pd.read_csv(folder_path.joinpath(filename_wind), sep=';', index_col=False)
        df2 = pd.read_csv(folder_path.joinpath(filename_wind_extreme), sep=';', index_col=False)

        # Remove leading and trailing spaces from column names
        df1.columns = df1.columns.str.strip()
        df2.columns = df2.columns.str.strip()

        # Ensure 'MESS_DATUM' is in the correct date format
        df1['MESS_DATUM'] = pd.to_datetime(df1['MESS_DATUM'], format='%Y%m%d%H%M')
        df2['MESS_DATUM'] = pd.to_datetime(df2['MESS_DATUM'], format='%Y%m%d%H%M')

        # Merge the two DataFrames based on the 'STATIONS_ID' and 'MESS_DATUM' columns
        merged_df = pd.merge(df1, df2, on=['STATIONS_ID', 'MESS_DATUM'], suffixes=('_file1', '_file2'))

        # Remove the 'eor' columns
        merged_df = merged_df.drop(['eor_file1', 'eor_file2'], axis=1)

        # Replace -999 with None as -999 is marked as missing value
        merged_df = merged_df.replace(-999, None)
        logger.debug(f"Loaded wind and extreme wind data from {filename_wind} and {filename_wind_extreme}!")
        return merged_df

    @staticmethod
    def load_and_prepare_station_metadata(station_id: int, folder_path: Union[str, Path],
                                          filename_stations_list: str) -> dict:
        """
        Load and prepare station data from a text file.

        :param station_id: The id of the station.
        :param folder_path: The folder where the file is located.
        :param filename_stations_list: The name of the file.
        :return: A dictionary containing station metadata.
        """
        # Create file path
        file_path = Path(folder_path) / filename_stations_list

        # Iterate through the lines of the file
        with open(file_path, 'r', encoding='latin1') as f:
            next(f)  # Skip the header line
            next(f)  # Skip the line with the separators
            for line in f:
                line_data = line.split()
                if int(line_data[0]) == station_id:
                    # Convert the found line into a dictionary
                    station_metadata = {
                        "station_id": int(line_data[0]),
                        "datetime_start": line_data[1],
                        "datetime_end": line_data[2],
                        "station_height": int(line_data[3]),
                        "station_latitude": float(line_data[4]),
                        "station_longitude": float(line_data[5]),
                        "station_name": " ".join(line_data[6:-1]),
                        "bundesland": line_data[-1]
                    }

                    logger.debug(f"Loaded station metadata from {filename_stations_list}: {station_metadata.__str__()}")
                    return station_metadata

        raise ValueError(f"No data found for station id {station_id}")

    @classmethod
    def _create_wind_measurement(cls, name: str, merged_df: pd.DataFrame, station_metadata: dict) -> Optional[
        "WindMeasurement"]:
        """
        Creates an instance of the WindMeasurement class from the provided data.

        Parameters
        ----------
        name : str
            Name of the WindMeasurement.
        merged_df : pandas.DataFrame
            Merged DataFrame with data.
        station_metadata : dict
            Dictionary containing station metadata.

        Returns
        -------
        WindMeasurement
            An instance of the WindMeasurement class filled with wind measurement data, or None if there's an error.
        """
        try:
            obj = cls()
            obj.wind_measurement_name = name
            # from metadata
            obj.station_id = station_metadata["station_id"]
            obj.station_name = station_metadata["station_name"]
            obj.bundesland = station_metadata["bundesland"]
            obj.station_height = station_metadata["station_height"]
            obj.station_latitude = station_metadata["station_latitude"]
            obj.station_longitude = station_metadata["station_longitude"]

            obj.source = f"https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/"
            obj.datetime_added = func.datetime('now')

            for idx, row in merged_df.iterrows():
                wind_data = WindData(
                    id=None,
                    wind_measurement_id=obj.wind_measurement_id,
                    datetime=row['MESS_DATUM'],
                    quality_level_wind_avg=row['QN_file1'],
                    wind_speed_10min_avg=row['FF_10'],
                    wind_direction_10min_avg=row['DD_10'],
                    quality_level_wind_extremes=row['QN_file2'],
                    wind_speed_max_10min=row['FX_10'],
                    wind_speed_min_10min=row['FNX_10'],
                    wind_speed_max_10min_moving_avg=row['FMX_10'],
                    wind_direction_max_wind_speed=row['DX_10']
                )
                obj.wind_data.append(wind_data)
            obj.update_metadata()
        except Exception as e:
            logger.error("Error while creating WindData objects: " + str(e))
            return None
        logger.debug(f"Successfully created object {obj.__str__()}!")
        return obj

    @staticmethod
    def _handle_db_session(session, wind_measurement, auto_commit: bool, overwrite: bool):
        """
        Handles the database session.

        Parameters
        ----------
        session : sqlalchemy.orm.Session
            Database session. If None, a new session will be created.
        wind_measurement : WindMeasurement
            WindMeasurement instance to handle in the session.
        auto_commit : bool
            Flag to indicate if the session should auto-commit.
        overwrite : bool
            Flag to indicate if existing data should be overwritten.

        Returns
        -------
        WindMeasurement
            The WindMeasurement if the operation was successful, None otherwise.
        """
        try:
            session = db_manager.get_session(session)

            existing_obj = session.query(WindMeasurement).filter_by(
                wind_measurement_name=wind_measurement.wind_measurement_name).first()
            if existing_obj is not None:
                if overwrite:
                    logger.warning("Existing object will be overwritten (overwrite = True): ")
                    session.delete(existing_obj)
                else:
                    logger.warning("Object already exists, not overwritten (overwrite = False), obj: ")
                    return existing_obj

            session.add(wind_measurement)

            if auto_commit:
                session.commit()
                logger.info("Loading data from csv and committing to database successful (dec_auto_commit=True)!")
            else:
                logger.info("Loading data from csv successful (dec_auto_commit=False)!")

            return wind_measurement
        except Exception as e:
            logger.error("Error while handling the database session: " + str(e))
            return None

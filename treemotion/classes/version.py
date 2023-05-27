# treemotion/classes/version.py
from sqlalchemy import text
from common_imports.classes_heavy import *
from utils.path_utils import validate_and_get_filepath
from utils.dataframe_utils import validate_df
from tms.time_limits import validate_time_format, limit_df_by_time, optimal_time_frame
from tms.find_peaks import find_max_peak, find_n_peaks

from .wind_measurement import WindMeasurement

logger = get_logger(__name__)


class Version(BaseClass):
    __tablename__ = 'Version'
    version_id = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    measurement_id = Column(Integer, ForeignKey('Measurement.measurement_id', onupdate='CASCADE'), nullable=False)
    version_name = Column(String)
    tms_table_name = Column(String)
    datetime_start = Column(DateTime)
    datetime_end = Column(DateTime)
    duration = Column(Float)
    length = Column(Integer)
    tempdrift_method = Column(String)
    peak_index = Column(Integer)
    peak_time = Column(DateTime)
    peak_value = Column(Float)

    measurement = relationship("Measurement", back_populates="version", lazy="joined")

    def __init__(self, *args, version_id: int = None, measurement_id: int = None, version_name: str = None,
                 tms_table_name: str = None, **kwargs):
        """
        Initializes a Data instance.

        :param version_id: Unique ID of the data.
        :param measurement_id: ID of the measurement to which the data belongs.
        :param version_name: Data version.
        :param df: DataFrame with the data.
        """
        super().__init__(*args, **kwargs)
        # in SQLite Database
        self.version_id = version_id
        self.measurement_id = measurement_id
        self.version_name = version_name
        self.tms_table_name = tms_table_name
        self.tms_wind_table_name = None
        self.datetime_start = None  # metadata
        self.datetime_end = None  # metadata
        self.duration = None  # metadata
        self.length = None  # metadata
        self.tempdrift_method = None  # metadata
        self.peak_index = None  # metadata
        self.peak_time = None  # metadata
        self.peak_value = None  # metadata
        # additional only in class-object
        self.peaks_indexes = None
        self.peaks_times = None
        self.peaks_values = None
        # additional only in class-object, own table in database "auto_{}_df_{version}_{id_measurement}_measurement
        self._tms_df = None
        self._wind_df = None
        self._tms_wind_df = None
        # self.update_metadata()

    def __str__(self):
        return f"{self.__class__.__name__}(id={self.version_id}, tms_table_name={self.tms_table_name})"

    def describe(self):
        """
        Provides a detailed description of the attributes of this Data instance.
        """
        try:
            print(f"Data ID: {self.version_id}")
            print(f"Measurement ID: {self.measurement_id}")
            print(f"Measurement Series ID: {self.measurement.series_id}")
            print(f"Sensor ID: {self.measurement.sensor_id}")
            print(f"Version: {self.version_name}")
            print(f"Table name in database: {self.tms_table_name}")
            print(f"Start datetime: {self.datetime_start.strftime('%d.%m.%Y %H:%M:%S')}")
            print(f"End datetime: {self.datetime_end.strftime('%d.%m.%Y %H:%M:%S')}")
            print(f"Duration: {self.duration} seconds")
            print(f"Rows in dataset: {self.length}")
            print(f"Temperature drift method: {self.tempdrift_method}")
            print(f"Peak Index: {self.peak_index}")
            print(f"Peak Time: {self.peak_time.strftime('%d.%m.%Y %H:%M:%S')}")
            print(f"Peak Value: {self.peak_value}")
            return True
        except Exception as e:
            raise e

    @property
    @dec_runtime
    def get_tms_df(self) -> Optional[pd.DataFrame]:
        """
        Loads TMS data from the database if it's not loaded yet.

        :return: The loaded TMS data.
        """
        logger.debug(f"{self.__class__.__name__}.get_tms_df running for {self}!")
        if not hasattr(self, '_tms_df') or self._tms_df is None:
            session = db_manager.get_session()
            try:
                self._tms_df = pd.read_sql_table(self.tms_table_name, session.bind)
                logger.info(f"{self.__class__.__name__}.tms_df loaded successfully: {self}")
            except Exception as e:
                logger.error(f"{self.__class__.__name__}.df could not be loaded: {self}, error: {e}")
                return None
        return self._tms_df

    def set_tms_df(self, tms_df: pd.DataFrame, update_metadata: bool = True) -> bool:
        """
        Sets the TMS data to the given DataFrame and updates the database.

        Args:
            tms_df (pd.DataFrame): The DataFrame to set as the TMS data.
            update_metadata (bool, optional): If True, metadata is updated. Defaults to True.

        Returns:
            bool: True if the operation was successful, False otherwise.

        """
        logger.debug(f"{self.__class__.__name__}.set_tms_df running!")
        try:
            validate_df(tms_df, columns=config.tms_df_columns)
        except Exception as e:
            logger.error(f"Error during validation of the TMS-DataFrame: {e}")
            return False

        if update_metadata:
            if not self.update_metadata(tms_df):
                return False

        self._tms_df = tms_df

        logger.debug(f"{self.__class__.__name__}.tms_df set/edit successfully: {self.__str__()}")
        return True

    @dec_runtime
    def commit_tms_df(self):
        try:
            db_manager.commit_df(df=self.get_tms_df(), table_name=self.tms_table_name)
            logger.info(f"{self.__class__.__name__}.commit_tms_df committed successfully: {self.__str__()}")
            return True
        except Exception as e:
            logger.error(f"{self.__class__.__name__}.commit_tms_df failed: {self.__str__()}, error: {str(e)}")
            return False

    @staticmethod
    def get_tms_table_name(version_name: str, id_measurement: int) -> str:
        """
        Generates a tms table name.

        :param version_name: Version of the data.
        :param id_measurement: ID of the measurement to which the data belongs.
        :return: New table name.
        """
        return f"auto_tms_df_{version_name}_{str(id_measurement).zfill(3)}_measurement"

    # @classmethod
    # @dec_runtime
    # def load_from_db(cls, version_id: Optional[Union[int, List[int]]] = None, version_name=None,
    #                  get_tms_df: bool = True) -> List['Version']:
    #     """
    #     Loads Version objects from the database based on the provided filters.
    #
    #     :param version_id: The id of the version to load_from_db.
    #     :param version_name: The name of the version to load_from_db.
    #     :param get_tms_df
    #     :return: A list of Version objects matching the provided filters.
    #     """
    #     filter_by = {}
    #     if version_id:
    #         filter_by['version_id'] = version_id
    #     if version_name:
    #         filter_by['name'] = version_name
    #
    #     if isinstance(version_id, list):
    #         objs = super().load_from_db(ids=version_id)
    #     else:
    #         objs = super().load_from_db(filter_by=filter_by or None)
    #
    #     if get_tms_df:
    #         objs = [obj.get_tms_df() for obj in objs]
    #
    #     return objs

    @classmethod
    def load_from_csv(cls, filepath: str, measurement_id: int, version_id: int = None,
                      version_name: str = config.default_load_from_csv_version_name, auto_commit: bool = True) -> \
            Optional['Version']:
        """
        Loads TMS Data from a CSV file.

        :param filepath: Path to the CSV file.
        :param version_id: Unique ID of the version.
        :param measurement_id: ID of the measurement to which the data belongs.
        :param version_name: Version Name of the data.
        :param auto_commit:
        :return: Data object.
        """
        if not filepath:
            logger.warning(f"Filepath Measurement ID '{measurement_id}' is None, process aborted.")
            return None
        tms_table_name = cls.get_tms_table_name(version_name, measurement_id)
        obj = cls(version_id=version_id, measurement_id=measurement_id, version_name=version_name,
                  tms_table_name=tms_table_name)

        tms_df = cls.read_csv_tms(filepath)
        obj.set_tms_df(tms_df, update_metadata=True)

        if auto_commit:
            obj.commit_tms_df()
            db_manager.commit()
        return obj

    @staticmethod
    @dec_runtime
    def read_csv_tms(filepath: str) -> Optional[pd.DataFrame]:
        """
        Reads data from a CSV file.

        :param filepath: Path to the CSV file.
        :return: DataFrame with the read data.
        """
        try:
            filepath = validate_and_get_filepath(filepath)
        except Exception as e:
            logger.error(f"Error validating filepath: {e}")
            raise e

        try:
            df = pd.read_csv(filepath, sep=";", parse_dates=["Time"], decimal=",", index_col=False)
        except pd.errors.ParserError as e:
            logger.error(f"Error while reading the file {filepath.stem}. Please check the file format.")
            raise e
        except Exception as e:
            logger.error(f"Unusual error while loading {filepath.stem}: {e}")
            raise e

        return df

    @dec_runtime
    def copy(self, reset_id: bool = False, auto_commit: bool = False) -> 'Version':
        """
        Create a copy of the data object.

        :param reset_id: Whether to reset the ID of the new object.
        :param auto_commit: Whether to auto-commit after copying.
        :return: Copied data object.
        """
        obj = super().copy(reset_id, auto_commit)
        tms_df = self.get_tms_df()
        obj.set_tms_df(tms_df.copy(deep=True))

        return obj

    # used by version_event_listener.py
    @dec_runtime
    def delete_tms_table(self) -> bool:
        """
        Deletes the TMS data table from the database.

        Returns:
            bool: True if the operation was successful, False otherwise.
        """
        session: Session = db_manager.get_session()

        try:
            statement = text(f"DROP TABLE IF EXISTS {self.tms_table_name}")
            session.execute(statement)
            logger.info(f"For {self.__str__()} delete_tms_table '{self.tms_table_name}' successful.")
            return True
        except Exception as e:
            logger.error(f"For {self.__str__()} delete_tms_table '{self.tms_table_name}' error: {e}")
            return False

    @classmethod
    def create_new_version(cls, source_obj, new_version_name: str, auto_commit):
        """
        Creates a new version of the Version object.

        :param source_obj: The source Version object to be copied.
        :param new_version_name: The new version.
        :param auto_commit:
        :return: New data object.
        """
        tms_table_name = cls.get_tms_table_name(new_version_name, id_measurement=source_obj.measurement_id)
        obj = cls(version_id=None, measurement_id=source_obj.measurement_id, version_name=new_version_name, tms_table_name=tms_table_name)
        try:
            tms_df = source_obj.get_tms_df().copy(deep=True)
            obj.set_tms_df(tms_df, update_metadata=True)

            session = db_manager.get_session()
            session.add(obj)
            if auto_commit:
                db_manager.commit(session)
                logger.debug(f"Created and auto committed the new instance '{obj}' to the database successful.")
            else:
                logger.debug(f"Created new Version '{obj}' successful.")
            return obj

        except Exception as e:
            logger.error(f"Error while creating a copy or committing of the data instance: {e}")
            return None

    def _validate_tms_df(self):
        """
        Checks if the DataFrame Data.df is valid and contains the required columns.
        """

        try:
            validate_df(self._tms_df, columns=config.tms_df_columns)
        except Exception as e:
            logger.error(f"Error during validation of the DataFrame: {e}")
            return False
        return True

    def update_metadata(self, tms_df):
        """
        Updates the metadata of the data object (Data.df).

        First checks if the DataFrame is valid. If the DataFrame is invalid, it returns False.
        If the DataFrame is valid, it updates the metadata and returns True.

        Parameters
        ----------
        tms_df : Pandas DataFrame


        Returns
        -------
        bool
            True if the metadata update was successful, False if the DataFrame is invalid or an error occurred.
        """

        try:
            self.datetime_start = pd.to_datetime(tms_df['Time'].min(), format='%Y-%m-%d %H:%M:%S.%f')
            self.datetime_end = pd.to_datetime(tms_df['Time'].max(), format='%Y-%m-%d %H:%M:%S.%f')
            self.duration = (self.datetime_end - self.datetime_start).total_seconds()
            self.length = len(tms_df)
            # peak = self.find_max_peak()
            # if peak is None:
            #     logger.warning(f"No peak found for {self}, updating other metadata for {self}!")
            #     return True
            # self.peak_index = peak['peak_index']
            # self.peak_time = peak['peak_time']
            # self.peak_value = peak['peak_value']
            logger.debug(f"Metadata updated successfully for {self}")
        except (KeyError, ValueError) as e:
            logger.error(f"Failed to update metadata for {self}: {e}")
            return False
        return True

    def find_max_peak(self, show_peak: bool = False, value_col: str = "Absolute-Inclination - drift compensated",
                      time_col: str = "Time"):
        result = self._validate_tms_df()
        if not result:
            return None
        try:
            peak = find_max_peak(self.tms_df, value_col, time_col)
        except Exception as e:
            logger.warning(f"No peak found for {self}, error: {e}")
            return None

        if show_peak:
            logger.info(f"Peak in {self}: {peak}")
        return peak

    def find_n_peaks(self, show_peaks: bool = False, values_col: str = 'Absolute-Inclination - drift compensated',
                     time_col: str = 'Time', n_peaks: int = 10, sample_rate: float = 20,
                     min_time_diff: float = 60, prominence: int = None):
        result = self._validate_tms_df()
        if not result:
            return None
        try:
            peaks = find_n_peaks(self.tms_df, values_col, time_col, n_peaks, sample_rate, min_time_diff, prominence)
        except Exception as e:
            logger.warning(f"No peaks found for {self.__str__()}, error: {e}")
            return None
        if show_peaks:
            logger.info(f"Peaks found in {self.__str__()}: {peaks.__str__()}")
        return peaks

    def limit_by_time(self, start_time: str, end_time: str, auto_commit: bool = False):
        """
        Limits the data to a specific time range.

        Parameters
        ----------
        start_time : str
            Start time of the limit.
        end_time : str
            End time of the limit.
        auto_commit :

        Returns
        -------
        bool
            True if successful, False if error occurs.
        """
        # Validate time format
        start_time = validate_time_format(start_time)
        end_time = validate_time_format(end_time)
        if start_time is None or end_time is None:
            logger.error("Invalid time format.")
            return False

        # Validate DataFrame
        if not self._validate_tms_df():
            return False

        # Limit time
        try:
            tms_df = self.get_tms_df
            tms_df = limit_df_by_time(tms_df, time_col="Time", start_time=start_time, end_time=end_time)
            self.set_tms_df(tms_df, update_metadata=True)
        except Exception as e:
            logger.error(f"Error limiting the data of '{self.__str__()}': {str(e)}")
            return False

        logger.debug(f"Successfully limited the data of '{self.__str__()}' between {start_time} and {end_time}.")

        if auto_commit:
            self.commit_tms_df()
            db_manager.commit()

        return True

    def limit_time_by_peaks(self, duration: int, values_col: str = 'Absolute-Inclination - drift compensated',
                            time_col: str = 'Time', n_peaks: int = 10,
                            sample_rate: float = 20, min_time_diff: float = 60,
                            prominence: int = None, auto_commit: bool = False):
        """
        Limits the data based on the peaks in a specified column.

        Parameters
        ----------
        duration : int
            Duration of the time frame to limit the data.
        values_col : str, optional
            Name of the column containing the values. Default is 'Absolute-Inclination - drift compensated'.
        time_col : str, optional
            Name of the column containing the time values. Default is 'Time'.
        n_peaks : int, optional
            Number of peaks to consider. Default is 10.
        sample_rate : float, optional
            Sample rate in Hz. Default is 20.
        min_time_diff : float, optional
            Minimum time difference between peaks in seconds. Default is 60.
        prominence : int, optional
            Prominence value for peak detection. If None, prominence is not used. Default is None.
        auto_commit:

        Returns
        -------
        bool
            True if the data was successfully limited, False otherwise.
        """
        # Check the DataFrame
        if self.tms_df is None or self.tms_df.empty:
            logger.warning(f"The DataFrame of {self.__str__()} is None or empty.")
            return False

        # Check if the columns exist in the DataFrame
        if values_col not in self.tms_df.columns or time_col not in self.tms_df.columns:
            logger.warning(f"The columns {values_col} and/or {time_col} do not exist in the DataFrame.")
            return False

        peaks_dict = find_n_peaks(self.tms_df, values_col, time_col, n_peaks, sample_rate, min_time_diff, prominence)

        timeframe_dict = optimal_time_frame(duration, peaks_dict)
        self.tms_df = limit_df_by_time(self.tms_df, time_col="Time", start_time=timeframe_dict['start_time'],
                                       end_time=timeframe_dict['end_time'])

        logger.info(
            f"Successfully limited the data of '{self.__str__()}' between {timeframe_dict['start_time']} and {timeframe_dict['end_time']}.")
        if auto_commit:
            db_manager.commit()

        return True

    def random_sample(self, n: int, auto_commit: bool = False):
        """
        Selects a random sample of data while preserving the original order.

        :param n: Number of data points to select.
        :param auto_commit:

        Returns
        -------
        Version
            Self-reference for method chaining.
        """
        logger.info(f"Selecting a random sample of {n} data points from {self.__str__()}.")

        if n > len(self.tms_df):
            logger.warning(
                f"The value of N ({n}) is larger than the length of the DataFrame ({len(self.tms_df)}). Selecting all data points.")
            n = len(self.tms_df)

        try:
            sampled_indices = self.tms_df.sample(n).index
            sampled_indices = sorted(sampled_indices)
            self.tms_df = self.tms_df.loc[sampled_indices]
            logger.debug(f"Selected a random sample of {n} data points: {self.__str__()}")
        except Exception as e:
            logger.error(f"Error while selecting the random sample: {e}")
            return self
        if auto_commit:
            db_manager.commit()
        return self

    def get_wind_df(self, wind_measurement_id, time_extension_secs=0):
        """
        Query the wind data based on a given wind measurement ID and store the resulting DataFrame.

        Parameters
        ----------
        wind_measurement_id : int
            The ID of the wind measurement for which the data is queried.
        time_extension_secs : int, optional
            Time extension in seconds by which the query time range is extended. Default is 0.

        Returns
        -------
        pandas.DataFrame or None
            A DataFrame containing the queried data, or None if no data was found.

        """
        try:
            session = db_manager.get_session()
            wind_measurement = session.query(WindMeasurement).filter_by(wind_measurement_id=wind_measurement_id).first()

            if wind_measurement is None:
                logger.warning(f'No wind measurement found with ID: {wind_measurement_id}')
                return None

            extended_datetime_start = self.datetime_start - timedelta(seconds=time_extension_secs)
            extended_datetime_end = self.datetime_end + timedelta(seconds=time_extension_secs)

            wind_df = wind_measurement.get_wind_df(extended_datetime_start, extended_datetime_end,
                                                   columns=config.wind_df_columns_selected, session=session)

            self._wind_df = wind_df
            logger.info(f'Successfully retrieved wind data for wind measurement with ID {wind_measurement_id}')
            return wind_df

        except Exception as e:
            logger.error(
                f'Error while retrieving wind data for wind measurement with ID {wind_measurement_id}: {str(e)}')
            raise e




# def sync_wind_df(self, wind_measurement_id, max_time_shift_secs=0, session=None):
#
#     tms_df = self.tms_df
#     wind_df = self.get_wind_df(wind_measurement_id, time_extension_secs=max_time_shift_secs, session=session)
#
#     if not self._validate_tms_df(wind_in_df=False):
#         logger.error(f"")
#         return None
#     if not validate_df(wind_df, columns=config.wind_df_columns_selected):
#         logger.error(f"")
#         return None
#
#     # tms_time_col =

#
# @dec_runtime
# def temp_drift_comp(self, method="emd_hht", overwrite=True, sample_rate=20, window_size=1000,
#                     freq_range=(0.05, 2, 128), feedback=False):  # 128 is used because ...
#     """
#     Compensate the temperature drift in the measurements using the specified method.
#
#     :param method: The method to use for temperature drift compensation.
#     :param overwrite: Whether to overwrite the original data or create new columns.
#     :param sample_rate: The sample rate of the data (in Hz).
#     :param window_size: The window size for the moving average method.
#     :param freq_range: The frequency range for the EMD-HHT method.
#     :param feedback: Show result and runtime
#     """
#     temp = self.df['Temperature']
#
#     methods = {
#         "lin_reg": lambda df: tempdrift.temp_drift_comp_lin_reg(df, temp),
#         "lin_reg_2": lambda df: tempdrift.temp_drift_comp_lin_reg_2(df, temp),
#         "mov_avg": lambda df: tempdrift.temp_drift_comp_mov_avg(df, window_size),
#         "emd_hht": lambda df: tempdrift.temp_drift_comp_emd(df, sample_rate, freq_range),
#     }
#
#     if method in methods:
#         x = methods[method](self.df['East-West-Inclination'])
#         y = methods[method](self.df['North-South-Inclination'])
#     else:
#         raise ValueError(f"Invalid method for temp_drift_comp: {method}")
#
#     suffix = "" if overwrite else " - new"
#
#     self.df[f'East-West-Inclination - drift compensated{suffix}'] = x
#     self.df[f'North-South-Inclination - drift compensated{suffix}'] = y
#     self.df[f'Absolute-Inclination - drift compensated{suffix}'] = tms_basics.get_absolute_inclination(x, y)
#     self.df[
#         f'Inclination direction of the tree - drift compensated{suffix}'] = tms_basics.get_inclination_direction(
#         x, y)
#     if feedback is True:
#         print(
#             f"Messung.temp_drift_comp - id_messung: {self.id_messung}")
#
# def plot_df(self, y_cols):
#     """
#     Plots the specified columns of df against time.
#
#     Args:
#         y_cols (list of str): The names of the columns to plot on the y-axis.
#
#     Returns:
#         None.
#
#     Raises:
#         KeyError: If any of the specified column names are not in the df.
#     """
#     fig, ax = plt.subplots()
#     x = self.df["Time"]
#     for col in y_cols:
#         try:
#             y = self.df[col]
#         except KeyError:
#             raise KeyError(f"Column '{col}' not found in df.")
#         ax.plot(x, y, label=col)
#     ax.set_xlabel("Time")
#     ax.legend()
#     plt.show()
#
# def plot_df_sub(self, y_cols):
#     """
#     Plot multiple time series subplots.
#
#     Parameters:
#     y_cols (list of str): The columns to plot.
#
#     Returns:
#     None
#
#     Raises:
#         KeyError: If any of the specified column names are not in the df.
#     """
#     num_plots = len(y_cols)
#     fig, axs = plt.subplots(num_plots, 1, figsize=(8, num_plots * 4))
#     x = self.df["Time"]
#     for i, col in enumerate(y_cols):
#         try:
#             y = self.df[col]
#         except KeyError:
#             raise KeyError(f"Column '{col}' not found in df.")
#         axs[i].plot(x, y, label=col)
#         axs[i].set_xlabel("Time")
#         axs[i].legend()
#     plt.show()

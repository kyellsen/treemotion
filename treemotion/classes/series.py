import pandas as pd
from typing import Tuple

from kj_core.utils.path_utils import validate_and_get_file_list, extract_sensor_id, extract_last_three_digits
from ..common_imports.imports_classes import *

from .measurement import Measurement
from .measurement_version import MeasurementVersion
from .data_wind_station import DataWindStation
from .base_class_data_tms import BaseClassDataTMS
from .data_tms import DataTMS
from .data_merge import DataMerge

import treemotion

logger = get_logger(__name__)


class Series(BaseClass):
    """
    This class represents a series in the system.
    """
    __tablename__ = 'Series'
    series_id = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    project_id = Column(Integer, ForeignKey('Project.project_id', onupdate='CASCADE'))
    description = Column(String)
    datetime_start = Column(DateTime)
    datetime_end = Column(DateTime)
    location = Column(String)
    data_wind_station_id = Column(Integer, ForeignKey('DataWindStation.data_id', onupdate='CASCADE'))
    note = Column(String)
    filepath_tms = Column(String)
    filepath_ls3 = Column(String)
    optimal_shift_sec_median = Column(Float)

    measurement = relationship(Measurement, backref="series", lazy="joined",
                               cascade='all, delete-orphan', order_by='Measurement.measurement_id')
    data_wind_station = relationship("DataWindStation", backref="series", uselist=False, cascade='all')

    def __init__(self, series_id=None, project_id=None, description=None, datetime_start=None,
                 datetime_end=None, location=None, note=None, filepath_tms=None, filepath_ls3=None,
                 optimal_shift_sec_median: int = None):
        super().__init__()
        self.series_id = series_id
        self.project_id = project_id
        self.description = description
        self.datetime_start = datetime_start
        self.datetime_end = datetime_end
        self.location = location
        self.note = note
        self.filepath_tms = filepath_tms
        self.filepath_ls3 = filepath_ls3

        self.optimal_shift_sec_median = optimal_shift_sec_median

        self._version_dict = {}

    def __str__(self):
        return f"Series(series_id={self.series_id}, location={self.location})"

    @dec_runtime
    def add_filenames(self, csv_path: str, auto_commit: bool = True):
        """
        Update the 'filename_tms' for each measurement in this series by searching for CSV files in the specified path.
        Logs the number of successful updates.
        """

        if not self.filepath_tms:
            logger.warning(f"No 'filepath_tms' in series {self.series_id}.")
            return

        # Step 0: Validate csv_path
        try:
            csv_path = Path(csv_path)
            if not csv_path.is_dir():
                logger.error(f"Provided path {csv_path} is not a directory.")
                return
        except Exception as e:
            logger.error(f"Invalid path: {csv_path}, Error: {e}")
            return

        # Step 1: Validate filepath_tms
        try:
            # Ensure filepath_tms is treated as a relative path
            if self.filepath_tms.startswith("/"):
                filepath_tms = Path(self.filepath_tms[1:])
            else:
                filepath_tms = Path(self.filepath_tms)
        except Exception as e:
            logger.error(f"Invalid filepath_tms: {self.filepath_tms}, Error: {e}")
            return

        search_path = csv_path.joinpath(filepath_tms)
        if not search_path.is_dir():
            logger.error(f"search_path {search_path} does not exist.")
            return

        # Step 2: Find CSV files in the specified path
        csv_files = validate_and_get_file_list(search_path, file_type="csv")
        if csv_files is None:
            logger.error(f"No CSV files found in {search_path}.")
            return

        # Step 3: Extract sensor IDs from filenames
        sensor_ids = extract_sensor_id(csv_files)
        if sensor_ids is None:
            return

        # Step 4: Update measurements
        successful_updates = 0
        for measurement in self.measurement:
            if measurement.sensor_id in sensor_ids:
                corresponding_file = next(
                    (f for f in csv_files if extract_last_three_digits(f) == measurement.sensor_id), None)
                if corresponding_file and corresponding_file.is_file():
                    measurement.filename_tms = str(corresponding_file.name)
                    measurement.filepath_tms = str(search_path / corresponding_file.name)
                    successful_updates += 1
                else:
                    logger.warning(f"No corresponding file for sensor ID {measurement.sensor_id} found or invalid.")
            else:
                logger.warning(f"Sensor ID {measurement.sensor_id} not found in the extracted sensor IDs.")

        if auto_commit:
            self.get_database_manager().commit()

        total_measurements = len(self.measurement)
        logger.info(f"Updated filenames for {successful_updates} out of {total_measurements} measurements.")

    @dec_runtime
    def add_wind_station(self,
                         station_id: str,
                         filename_wind: Optional[str] = None,
                         filename_wind_extreme: Optional[str] = None,
                         filename_stations_list: Optional[str] = None,
                         auto_commit: bool = True,
                         update_existing: bool = False):
        """
        Adds or updates a wind station in the database.

        :param station_id: Identifier for the station.
        :param filename_wind: Alternative filename for wind data.
        :param filename_wind_extreme: Alternative filename for wind extreme data.
        :param filename_stations_list: Alternative filename for stations list.
        :param auto_commit: If True, commits the transaction automatically.
        :param update_existing: If True, overwrites the existing wind station data; otherwise, retains the existing data.
        :return: DataWindStation instance that was added or found in the database.
        """
        logger.info(f"Processing add_wind_station for '{self}'")
        session = self.get_database_manager().session

        try:
            # Check for an existing DataWindStation with the given station_id
            existing_station: DataWindStation = session.query(DataWindStation).filter(
                DataWindStation.station_id == station_id).first()

            if existing_station and not update_existing:
                # Return the existing DataWindStation without creating a new one
                logger.debug(f"Return existing DataWindStation, overwrite = 'False: '{existing_station}'")
                data_wind_station = existing_station

            else:
                # Create a new instance or update the existing one
                if existing_station:
                    data_wind_station = existing_station.update_from_dwd(filename_wind,
                                                                         filename_wind_extreme,
                                                                         filename_stations_list)
                    logger.debug(f"Update existing DataWindStation, overwrite = 'True: '{existing_station}'")
                else:
                    data_wind_station = DataWindStation.create_from_station(station_id, filename_wind,
                                                                            filename_wind_extreme,
                                                                            filename_stations_list)
                    session.add(data_wind_station)
                    logger.debug(f"Created new DataWindStation: '{data_wind_station}'")

            session.flush()
            self.data_wind_station_id = data_wind_station.data_id

            if auto_commit:
                self.get_database_manager().commit()
            return data_wind_station

        except Exception as e:
            logger.error(f"Error in add_wind_station: {e}")
            raise  # Optionally re-raise the exception to notify calling functions

    @dec_runtime
    def get_measurement_version_by_filter(self, filter_dict: Dict[str, Any], method: str = "list_filter") \
            -> Optional[List[MeasurementVersion]]:
        """
        Overwrites BaseClass Method. It´s way faster.
        Finds all MeasurementVersion objects associated with measurements related to this series based on the provided filters.

        :param filter_dict: A dictionary of attributes and their desired values.
                            For example: {'tms_table_name': tms_table_name}
        :param method: The method to use for filtering. Possible values are "list_filter" and "db_filter".
                       The default value is "list_filter".
        :return: A list of found MeasurementVersion instances, or None if no match is found.
        """

        if not isinstance(filter_dict, dict):
            logger.error("Input filter is not a dictionary. Please provide a valid filter dictionary.")
            return None

        try:
            if method == "list_filter":
                matching_mv_list: List= [mv for measurement in self.measurement for mv in measurement.measurement_version if
                                    all(getattr(mv, k, None) == v for k, v in filter_dict.items())]
            elif method == "db_filter":
                session = self.get_database_manager().session()
                matching_mv_list: List = (session.query(MeasurementVersion)
                                          .join(Measurement, MeasurementVersion.measurement_id == Measurement.id)
                                          .filter(Measurement.series_id == self.series_id)
                                          .filter_by(**filter_dict)
                                          .all())
            else:
                raise ValueError("Invalid method. Please choose between 'list_filter' and 'db_filter'.")

            if len(matching_mv_list) <= 0:
                raise ValueError(f"No measurement_version found for filter: '{filter_dict}'")

            return matching_mv_list

        except Exception as e:
            logger.error(f"An error occurred while querying the Version: {e}")
            return None

    @dec_runtime
    def calc_optimal_shift_median(self, measurement_version_name: str = None, filter_min_corr: float = 0.5) -> Tuple[
        pd.DataFrame, float]:
        """
        Calculates the median of the optimal shift in seconds for a specified measurement version,
        considering only those measurements with a maximum correlation above a specified threshold.

        Args:
            measurement_version_name (str, optional): The name of the measurement version. If None, uses the default from configuration.
            filter_min_corr (float, optional): The minimum correlation threshold to filter measurements. Defaults to 0.5.

        Returns:
            Tuple[pd.DataFrame, float]: A tuple containing a DataFrame with the detailed calculation results for each measurement
            and the median of the optimal shift in seconds for measurements above the correlation threshold.
        """
        try:
            # Use default measurement version name if not specified
            measurement_version_name = measurement_version_name or self.get_config().MeasurementVersion.measurement_version_name_default

            results = []  # Initialize list to store result dictionaries

            # Retrieve list of MeasurementVersion instances based on the specified name
            mv_list: List[MeasurementVersion] = self.get_measurement_version_by_filter(
                filter_dict={'measurement_version_name': measurement_version_name})

            for mv in mv_list:
                try:
                    optimal_shift, optimal_shift_sec, corr_shift_0, max_corr = mv.calc_optimal_shift()
                    results.append({
                        'optimal_shift': optimal_shift,
                        'optimal_shift_sec': optimal_shift_sec,
                        'initial_corr': corr_shift_0,  # Renamed for clarity
                        'max_corr': max_corr
                    })
                except Exception as e:
                    logger.error(f"Error calculating optimal shift for {mv}: {e}")

            # Convert results to DataFrame
            optimal_shift_df = pd.DataFrame(results)

            try:
                # Filter DataFrame to keep rows with 'max_corr' above specified threshold
                filtered_df = optimal_shift_df[optimal_shift_df['max_corr'] >= filter_min_corr]

                # Calculate median of 'optimal_shift_sec' from the filtered DataFrame
                optimal_shift_sec_median = filtered_df['optimal_shift_sec'].median()

                # Store the median of optimal shift seconds for further reference
                self.optimal_shift_sec_median = optimal_shift_sec_median

                logger.info("Successfully calculated optimal shift median.")
                return optimal_shift_df, optimal_shift_sec_median
            except Exception as e:
                logger.error(f"Error filtering data or calculating median: {e}")
                raise  # Re-raise exception after logging

        except Exception as e:
            logger.critical(f"Critical error in calc_optimal_shift_median: {e}")
            raise  # Ensuring that the exception is not silently swallowed

    @dec_runtime
    def cut_by_time(self, start_time: str, end_time: str, measurement_version_name: Optional[str] = None,
                    data_class_name: str = None, inplace: bool = False,
                    auto_commit: bool = False) -> Optional[List[pd.DataFrame]]:
        """
        Limits the measurement versions in the series by a specified time range.

        Args:
            start_time (str): Start time of the range, in a compatible format.
            end_time (str): End time of the range, in a compatible format.
            measurement_version_name (Optional[str]): Name of the measurement version.
                Defaults to a system-defined default measurement version name.
            data_class_name (str): Name of the subclass of BaseClassDataTMS related to measurement_version.
                Options are 'data_tms' or 'data_merge' (default).
            inplace (bool): If True, updates the instance's data in-place. Defaults to False.
            auto_commit (bool): If True, automatically commits changes to the database. Defaults to False.

        Returns:
            Optional[List[pd.DataFrame]]: List of DataFrame objects limited by the time range, or None if an error occurs.
        """
        # Default param-values as fallback
        config = self.get_config()
        measurement_version_name = measurement_version_name or config.MeasurementVersion.measurement_version_name_default
        data_class_name = data_class_name or config.Series.default_data_class_name
        try:
            # Retrieve list of MeasurementVersion instances based on the specified name
            mv_list: List[MeasurementVersion] = self.get_measurement_version_by_filter(
                filter_dict={'measurement_version_name': measurement_version_name})

            result = []
            for mv in mv_list:
                try:
                    mv.validate_data_class_name(data_class_name)
                    data_instance: BaseClassDataTMS = getattr(mv, data_class_name, None)
                    df: pd.DataFrame = data_instance.cut_by_time(start_time, end_time, inplace, auto_commit)
                    result.append(df)
                except Exception as e:
                    logger.error(f"Error processing measurement version '{mv}': {e}")

            logger.info(
                f"{self} successfully limited {len(result)} instances of Measurement_Version '{measurement_version_name}' by time.")
            return result

        except Exception as e:
            logger.error(f"Error occurred during cut_by_time: {e}")
            return None

    @dec_runtime
    def cut_time_by_peaks(self, measurement_version_name: Optional[str] = None,
                          data_class_name: str = None, duration: float = None, inplace: bool = False,
                          auto_commit: bool = False) -> Optional[Tuple[List[pd.DataFrame], pd.Series]]:
        """
        Begrenzt die Zeiten basierend auf Peaks in den gegebenen Daten.

        Args:
            measurement_version_name (Optional[str]): Name of the measurement version.
                Defaults to a system-defined default measurement version name.
            data_class_name (str): Name of the subclass of BaseClassDataTMS related to measurement_version.
                Options are 'data_tms' or 'data_merge' (default).
            duration (float):
            inplace (bool): If True, updates the instance's data in-place. Defaults to False.
            auto_commit (bool): If True, automatically commits changes to the database. Defaults to False.

        Returns:
            Optional[Tuple[List[pd.DataFrame], pd.Series]]: Tuple containing a list of DataFrame objects limited by the time range, and the Series of all peaks used for the time limit, or None if an error occurs.
        """
        # Default param-values as fallback
        config = self.get_config()
        measurement_version_name = measurement_version_name or config.MeasurementVersion.measurement_version_name_default
        data_class_name = data_class_name or config.Series.default_data_class_name
        duration = duration or config.Series.cut_time_by_peaks_duration

        try:
            if duration is None or duration <= 0:
                raise ValueError("duration must be greater than 0.")

            # Retrieve list of MeasurementVersion instances based on the specified name
            mv_list: List[MeasurementVersion] = self.get_measurement_version_by_filter(
                filter_dict={'measurement_version_name': measurement_version_name})

            peaks_list: List[pd.Series] = []
            for mv in mv_list:
                try:
                    mv.validate_data_class_name(data_class_name)
                    data_instance: BaseClassDataTMS = getattr(mv, data_class_name, None)
                    peaks: pd.Series = data_instance.peak_n
                    peaks_list.append(peaks)
                except Exception as e:
                    logger.error(f"Error processing measurement version '{mv}': {e}")
            all_peaks: pd.Series = pd.concat(peaks_list).sort_index()

            # Verwende all_peaks.index direkt für die Zeitpunkte der Peaks
            peak_times = all_peaks.index

            start_time, end_time = self._find_optimal_time_frame(duration, peak_times)
            result = self.cut_by_time(start_time, end_time, measurement_version_name, data_class_name, inplace,
                                      auto_commit)

            return result, all_peaks

        except Exception as e:
            logger.error(f"Error occurred during cut_by_time: {e}")
            return None

    @staticmethod
    def _find_optimal_time_frame(duration: float, peak_times: pd.DatetimeIndex) -> Tuple[str, str]:
        if peak_times.empty:
            raise ValueError("The peak_times Series is empty.")

        optimal_start_time = peak_times[0]
        optimal_end_time = optimal_start_time + pd.Timedelta(seconds=duration)
        max_peak_count = 0

        for start_time in peak_times:
            end_time = start_time + pd.Timedelta(seconds=duration)
            peak_count = peak_times[(peak_times >= start_time) & (peak_times < end_time)].size
            #logger.debug(f"Timeframe: {start_time} - {end_time}, duration {start_time - end_time}, peak_count {peak_count}")
            if peak_count > max_peak_count:
                max_peak_count = peak_count
                optimal_start_time = start_time
                optimal_end_time = end_time
                logger.debug(f"Better timeframe: {start_time} - {end_time}, duration {start_time - end_time}, peak_count {peak_count}")

        # Umwandeln der optimalen Zeiten in das korrekte Format
        optimal_start_time = optimal_start_time.strftime("%Y-%m-%d %H:%M:%S.%f")
        optimal_end_time = optimal_end_time.strftime("%Y-%m-%d %H:%M:%S.%f")
        logger.info(f"Found optimal timeframe: {optimal_start_time} - {optimal_end_time}, max_peak_count {max_peak_count}")

        return optimal_start_time, optimal_end_time

#from kj_core.df_utils.sample_rate import calc_sample_rate

from ..common_imports.imports_classes import *
from treemotion.tms.df_merge_by_time import merge_dfs_by_time, calc_optimal_shift

from .data_tms import DataTMS
from .data_merge import DataMerge

from ..plotting.plot_measurement_version import plot_wind_shift, plot_wind_shift_reg_linear, plot_wind_shift_reg_exp

logger = get_logger(__name__)


class MeasurementVersion(BaseClass):
    __tablename__ = 'MeasurementVersion'
    measurement_version_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    measurement_version_name = Column(String)
    measurement_id = Column(Integer, ForeignKey('Measurement.measurement_id', onupdate='CASCADE'), nullable=False)

    optimal_shift = Column(Integer)
    optimal_shift_sec = Column(Integer)
    corr_shift_0 = Column(Float)
    max_corr = Column(Float)

    data_tms = relationship("DataTMS", backref="measurement_version", uselist=False, cascade='all, delete-orphan')
    data_ls3 = relationship("DataLS3", backref="measurement_version", uselist=False, cascade='all, delete-orphan')
    data_merge = relationship("DataMerge", backref="measurement_version", uselist=False, cascade='all, delete-orphan')

    valid_data_attributes: List[str] = ["data_tms", "data_merge", "data_ls3"]

    def __init__(self, measurement_version_id=None, measurement_version_name=None, measurement_id=None,
                 optimal_shift=None, optimal_shift_sec=None, corr_shift_0=None, max_corr=None,
                 data_tms_id=None, data_ls3_id=None, data_merge_id=None):
        super().__init__()
        self.measurement_version_id = measurement_version_id
        self.measurement_version_name = measurement_version_name
        self.measurement_id = measurement_id

        self.optimal_shift = optimal_shift
        self.optimal_shift_sec = optimal_shift_sec
        self.corr_shift_0 = corr_shift_0
        self.max_corr = max_corr

        self.data_tms_id = data_tms_id
        self.data_ls3_id = data_ls3_id
        self.data_merge_id = data_merge_id

    def __str__(self):
        return f"{self.__class__.__name__}(id={self.measurement_version_id}, measurement_version_name={self.measurement_version_name})"

    def get_filename(self) -> str:
        filename = f"{self.__class__.__name__}"
        return filename

    @classmethod
    def load_tms_from_csv(cls, filepath_tms: str, measurement_id: int, measurement_version_id: int = None,
                          measurement_version_name: str = None) -> Optional['MeasurementVersion']:
        """
        Loads TMS Data from a CSV file.

        :param filepath_tms: Path to the CSV file.
        :param measurement_version_id: Unique ID of the version.
        :param measurement_id: ID of the measurement to which the data belongs.
        :param measurement_version_name: Version Name of the data.
        :return: MeasurementVersion object.
        """

        obj = cls(measurement_id=measurement_id, measurement_version_name=measurement_version_name)

        data_directory = cls.get_config().data_directory
        folder = cls.get_config().Data.data_tms_directory
        filename = (cls.
                    get_data_manager().
                    get_new_filename(measurement_id,
                                     prefix=f"tms_{measurement_version_name}",
                                     file_extension="feather"))
        data_tms_filepath = data_directory / folder / filename

        data_tms = DataTMS(data_id=None, data=DataTMS.read_csv_tms(filepath=filepath_tms),
                           data_filepath=str(data_tms_filepath),
                           measurement_version_id=measurement_version_id)

        obj.data_tms = data_tms

        session = obj.get_database_manager().session
        session.add(obj)
        return obj

    def validate_data_class_name(self, data_class_name: str):
        if data_class_name not in self.valid_data_attributes:
            raise ValueError(
                f"Invalid data_class_name '{data_class_name}'. Expected one of {self.valid_data_attributes}.")

    @property
    def data_wind_station(self) -> pd.DataFrame:
        """Returns the wind station data DataFrame."""
        if self.measurement.series.data_wind_station is None:
            error_msg = "Ensure series.data_wind_station is set properly before using! Call series.add_wind_station first!"
            logger.error(error_msg)
            raise ValueError(error_msg)
        return self.measurement.series.data_wind_station

    @property
    def shift_sec_median(self) -> float:
        """Returns the median shift in seconds."""
        shift_sec_median = self.measurement.series.optimal_shift_sec_median
        if shift_sec_median is None:
            error_msg = f"shift_sec is '{shift_sec_median}', set shift_sec manually or call series.calc_optimal_shift_median first."
            logger.error(error_msg)
            raise ValueError(error_msg)
        logger.info(f"Using optimal_shift_sec_median: {shift_sec_median}")
        return shift_sec_median

    @dec_runtime
    def calc_optimal_shift(self, max_shift_sec=None) -> Tuple[int, float, float, float]:
        """
        Calculates the optimal shift between wind and TMS data series within a specified time range.

        param: max_shift_sec

        :return: A tuple containing optimal shift in index values, optimal shift in seconds, initial correlation without shift, and maximum correlation.
        """
        logger.info("Starting calculation of optimal shift.")

        config = self.get_config().Data

        tms_series: pd.Series = self.data_tms.data[config.main_tms_value].copy()
        wind_series: pd.Series = self.data_wind_station.data[config.main_wind_value].copy()

        wind_series = wind_series.reindex(tms_series.index, method='nearest')

        tms_series = tms_series.rolling(window=config.time_rolling_max, closed='right').max()
        wind_series = wind_series.rolling(window=config.time_rolling_max, closed='right').max()

        tms_series.dropna(inplace=True)
        wind_series.dropna(inplace=True)

        sample_rate = config.tms_sample_rate_hz  # calc_sample_rate((tms_series + wind_series) / 2)
        max_shift_sec = max_shift_sec or config.max_shift_sec
        max_shift = round(sample_rate * max_shift_sec)

        logger.info(
            f"Input: sample_rate: '{sample_rate:.4f} Hz', max_shift_sec: '{max_shift_sec} s', "
            f"max_shift: '{max_shift} samples'")

        # Calculate optimal shift
        optimal_shift, correlation_no_shift, correlation_optimal_shift = calc_optimal_shift(tms_series, wind_series,
                                                                                            max_shift)

        optimal_shift_sec = optimal_shift / sample_rate

        logger.info(
            f"Output: optimal_shift: '{optimal_shift:.4f} samples', optimal_shift_sec: '{optimal_shift_sec:.4f} s', "
            f"correlation_no_shift: '{correlation_no_shift:.4f}', correlation_optimal_shift: '{correlation_optimal_shift:.4f}'")

        return optimal_shift, optimal_shift_sec, correlation_no_shift, correlation_optimal_shift

    def _get_rolling_max(self, merged_data: pd.DataFrame) -> pd.DataFrame:
        """
        Applies a rolling maximum function to specified columns in the DataFrame.

        This method updates the DataFrame 'merged_data' by replacing specified columns with their rolling maximum values.

        :param merged_data: The DataFrame to which the rolling maximum will be applied.
        :return: The updated DataFrame with rolling maximum values.
        """
        config = self.get_config().Data

        # Apply rolling max to the specified TMS and wind columns
        for value_col, merged_col in [(config.main_tms_value, config.merge_tms_value),
                                      (config.main_wind_value, config.merge_wind_value)]:
            merged_data[merged_col] = merged_data[value_col].rolling(
                window=config.time_rolling_max, closed='right').max()

        return merged_data

    @dec_runtime
    def sync_wind_tms_data(self, shift_sec: float = None) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Synchronizes wind and TMS data by applying an optimal time shift to the wind data.

        :return: A tuple of merged DataFrame without shift (for reference) and the shifted DataFrame.
        """
        logger.info("Starting synchronization of wind and TMS data.")

        config = self.get_config().Data

        shift_sec = shift_sec or self.shift_sec_median

        tms_df: pd.DataFrame = self.data_tms.data.copy()
        wind_df: pd.DataFrame = self.data_wind_station.data.copy()

        # Merge without shift for reference
        merged_df: pd.DataFrame = merge_dfs_by_time(tms_df, wind_df)

        # Calculate the shift in index values for wind_df
        freq = wind_df.index.inferred_freq
        if freq is not None and not any(char.isdigit() for char in freq):
            freq = '1' + freq  # Prepend '1' if freq is only a unit abbreviation (e.g., 'T' becomes '1T')
        try:
            wind_sampling_interval = pd.to_timedelta(freq).total_seconds()
            index_shift = round(shift_sec / wind_sampling_interval)
            logger.info(
                f"Calculated index shift for wind_df: {index_shift} index-values based on optimal shift of {shift_sec} seconds.")
        except ValueError as e:
            logger.error(f"Error converting frequency to timedelta: {e}")
            raise

        # Shift specified columns in wind_df
        columns_to_shift = config.data_wind_columns
        for column in columns_to_shift:
            if column in wind_df.columns:
                wind_df[column] = wind_df[column].shift(index_shift)
            else:
                logger.warning(f"Column '{column}' not found in wind_df. Skipping shift for this column.")

        # Merge after applying shift
        shifted_data: pd.DataFrame = merge_dfs_by_time(tms_df, wind_df)

        # Apply rolling max to both DataFrames
        self._get_rolling_max(merged_df)
        self._get_rolling_max(shifted_data)

        logger.info("Completed data synchronization. Returning merged data and shifted data.")
        return merged_df, shifted_data

    @dec_runtime
    def plot_shift_sync_wind_tms(self, mode: str = "median", plot_shift: bool = True, plot_linear: bool = False,
                                 plot_exponential: bool = True):
        config = self.get_config().Data

        try:
            if mode == "median":
                shift_sec = self.shift_sec_median
                corr_shift_0, max_corr = np.NAN, np.NAN
            elif mode == "single":
                shift, shift_sec, corr_shift_0, max_corr = self.calc_optimal_shift()
            else:
                logger.error(f"Invalid mode: {mode}. Use 'median' or 'single'.")
                raise ValueError("Invalid mode. Use 'median' or 'single'.")

            merged_data, shifted_data = self.sync_wind_tms_data(shift_sec=shift_sec)

            if plot_shift:
                fig = plot_wind_shift(self.measurement_id, merged_data, shifted_data,
                                      tms_col=config.merge_tms_value,
                                      wind_col=config.merge_wind_value,
                                      shift_sec=shift_sec,
                                      corr_shift_0=corr_shift_0,
                                      max_corr=max_corr)

                self.get_plot_manager().save_plot(fig,
                                                  filename=f"{mode}_{self.measurement_version_name}_{self.measurement_version_id}",
                                                  subdir=f"measurement_version/plot_wind_shift/time_{mode}")
                logger.info(f"Plot plot_wind_shift")
            if plot_linear:
                fig = plot_wind_shift_reg_linear(self.measurement_id, shifted_data,
                                                 tms_col=config.merge_tms_value,
                                                 wind_col=config.merge_wind_value)
                self.get_plot_manager().save_plot(fig,
                                                  filename=f"linear_{mode}_{self.measurement_version_name}_{self.measurement_version_id}",
                                                  subdir=f"measurement_version/plot_wind_shift/reg_lin_{mode}")
                logger.info(f"Plot plot_linear")
            if plot_exponential:
                fig = plot_wind_shift_reg_exp(self.measurement_id, shifted_data,
                                              tms_col=config.merge_tms_value,
                                              wind_col=config.merge_wind_value)
                self.get_plot_manager().save_plot(fig,
                                                  filename=f"exp_{mode}_{self.measurement_version_name}_{self.measurement_version_id}",
                                                  subdir=f"measurement_version/plot_wind_shift/reg_exp_{mode}")
                logger.info(f"Plot plot_exponential")

            logger.info(f"plot_shift_sync_wind_tms for {self.__class__.__name__}: '{self}' completed successfully.")

        except Exception as e:
            logger.error(f"Failed to plot_shift_sync_wind_tms: '{self}'. Error: {e}")
            raise

    @dec_runtime
    def add_data_merge(self, update_existing: bool = True) -> Optional[DataMerge]:
        logger.info(f"Processing add_data_merge for '{self}'")
        session = self.get_database_manager().session

        try:
            # Check for an existing DataMerge with the given measurement_version_id
            existing_data: DataMerge = session.query(DataMerge).filter(
                DataMerge.measurement_version_id == self.measurement_version_id).first()

            if existing_data and not update_existing:
                data_merge = existing_data
                session.flush()  # Maybe Cause of Error?
                logger.warning(
                    f"Return existing {data_merge.__class__.__name__}, update_existing = '{update_existing}': '{data_merge}'")

            else:
                if not existing_data:  # Create a new instance
                    _, shifted_data = self.sync_wind_tms_data()
                    data_merge = DataMerge.create_from_measurement_version(
                        measurement_version_id=self.measurement_version_id,
                        data=shifted_data)
                    session.flush()
                    logger.info(f"Created new {DataMerge.__class__.__name__}: '{data_merge}'")

                else:  # Update existing  instance
                    data_merge = existing_data
                    _, shifted_data = self.sync_wind_tms_data()
                    data_merge.update_from_measurement_version(data=shifted_data)
                    session.flush()
                    logger.warning(
                        f"Update existing {DataMerge.__class__.__name__}, update_existing = '{update_existing}': '{data_merge}'")
            self.data_merge = data_merge
            self.get_database_manager().commit()

            return data_merge

        except Exception as e:
            logger.error(f"Error in add_data_merge: {e}")
            raise  # Optionally re-raise the exception to notify calling functions

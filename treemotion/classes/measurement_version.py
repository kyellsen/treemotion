from ..common_imports.imports_classes import *
from treemotion.tms.df_merge_by_time import cut_df_to_match_length, merge_dfs_by_time, calc_optimal_shift, \
    calc_correlation_at_shift

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

    @dec_runtime
    def calc_optimal_shift(self) -> Tuple[int, float, float, float]:
        """
        Calculates the optimal shift between wind and TMS data series within a specified time range.

        :return: A tuple containing optimal shift in index values, optimal shift in seconds, initial correlation without shift, and maximum correlation.
        """
        logger.info("Starting calculation of optimal shift.")

        config = self.get_config().Data

        tms_df, wind_df = self._get_wind_tms_data_dfs()

        # Match lengths and merge dataframes
        wind_df = cut_df_to_match_length(wind_df, tms_df)
        merged_data = merge_dfs_by_time(tms_df, wind_df)

        # Resample and select relevant columns
        new_freq = config.calc_optimal_shift_down_sample_rate
        merged_data = merged_data[[config.main_tms_value, config.main_wind_value]]
        merged_data = merged_data.resample(new_freq).max()
        self._get_rolling_max(merged_data)

        # Determine sampling frequency in seconds
        sampling_freq_seconds = pd.to_timedelta(new_freq).total_seconds()
        max_shift = round(config.max_shift_sec / sampling_freq_seconds)

        logger.info(
            f"Input parameters: max_shift: '{max_shift}' index-values, max_shift_sec: '{config.max_shift_sec}' sec, sampling_freq: '{new_freq}', sampling_freq_seconds: '{sampling_freq_seconds}' per sec")

        # Calculate initial correlation without shift
        corr_shift_0 = calc_correlation_at_shift(merged_data, config.merge_tms_value, config.merge_wind_value, 0)
        logger.info(f"Initial correlation without shift: {corr_shift_0}")

        # Calculate optimal shift
        max_corr, optimal_shift = calc_optimal_shift(merged_data, config.merge_tms_value, config.merge_wind_value,
                                                     max_shift)

        # Convert optimal shift from index values to seconds
        optimal_shift_sec = optimal_shift * sampling_freq_seconds

        logger.info(
            f"Output: Optimal shift: {optimal_shift} index-values, Optimal shift in seconds: {optimal_shift_sec} sec, Maximum correlation: {max_corr}")

        return optimal_shift, optimal_shift_sec, corr_shift_0, max_corr

    def _get_wind_tms_data_dfs(self):
        # Ensure dataframes are independent copies
        tms_df = self.data_tms.data.copy()

        if self.measurement.series.data_wind_station is None:
            logger.critical(
                f"Ensure series.data_wind_station is set properly before using data_wind_station.data! Call series.add_wind_station first!")
            raise ValueError
        else:
            wind_df = self.measurement.series.data_wind_station.data.copy()

        return tms_df, wind_df

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
        tms_df, wind_df = self._get_wind_tms_data_dfs()

        if shift_sec is None:
            shift_sec = self._get_optimal_shift_sec_median()

        # Match the length of wind_df to tms_df
        wind_df = cut_df_to_match_length(wind_df, tms_df)

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

        # Merge without shift for reference
        merged_data: pd.DataFrame = merge_dfs_by_time(tms_df, wind_df)

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
        self._get_rolling_max(merged_data)
        self._get_rolling_max(shifted_data)

        logger.info("Completed data synchronization. Returning merged data and shifted data.")
        return merged_data, shifted_data

    def _get_optimal_shift_sec_median(self):
        series = self.measurement.series
        shift_sec: float = series.optimal_shift_sec_median
        logger.info(f"Using optimal_shift_sec_median from '{series}', shift_sec: '{shift_sec}'")
        if shift_sec is None:
            logger.error(
                f"shift_sec is '{shift_sec}', set shift_sec manuell or call {series.__class__.__name__}.calc_optimal_shift_median first.")
            raise ValueError
        return shift_sec

    @dec_runtime
    def plot_shift_sync_wind_tms(self, mode: str = "median", plot_shift: bool = True, plot_linear: bool = False,
                                 plot_exponential: bool = True):
        config = self.get_config().Data

        try:
            if mode == "median":
                shift_sec = self._get_optimal_shift_sec_median()
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
                session.flush() # Maybe Cause of Error?
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

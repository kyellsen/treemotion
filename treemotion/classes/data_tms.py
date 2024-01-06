from kj_core.classes.core_data_class import CoreDataClass
from kj_core.utils.df_utils import validate_df

from ..common_imports.imports_classes import *

from ..tms.find_peaks import find_max_peak, find_n_peaks
logger = get_logger(__name__)


class DataTMS(CoreDataClass, BaseClass):
    __tablename__ = 'DataTMS'
    data_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    data_filepath = Column(String, unique=True)
    measurement_version_id = Column(Integer,
                                    ForeignKey('MeasurementVersion.measurement_version_id', onupdate='CASCADE'),
                                    nullable=False)
    tempdrift_method = Column(String)

    def __init__(self, data_id: int = None, data: pd.DataFrame = None, data_filepath: str = None, datetime_added=None,
                 datetime_last_edit=None, measurement_version_id: int = None, tempdrift_method: str = None):
        CoreDataClass.__init__(self, data_id=data_id, data=data, data_filepath=data_filepath,
                               datetime_added=datetime_added, datetime_last_edit=datetime_last_edit)

        self.measurement_version_id = measurement_version_id

        self.tempdrift_method = tempdrift_method

    @staticmethod
    @dec_runtime
    def read_csv_tms(filepath: str) -> Optional[pd.DataFrame]:
        """
        Reads data from a CSV file.

        :param filepath: Path to the CSV file.
        :return: DataFrame with the read data.
        """
        filepath = Path(filepath)
        if not filepath.exists():
            logger.error(f"search_path {filepath} does not exist.")
            return

        try:
            tms_df = pd.read_csv(filepath, sep=";", parse_dates=["Time"], decimal=",", index_col=False)
        except pd.errors.ParserError as e:
            logger.error(f"Error while reading the file {filepath.stem}. Please check the file format.")
            raise e
        except Exception as e:
            logger.error(f"Unusual error while loading {filepath.stem}: {e}")
            raise e
        return tms_df

    @property
    def datetime_start(self):
        datetime_column_name = self.get_config().DataTMS.datetime_column_name
        if self.data is not None and datetime_column_name in self.data.columns:
            return self.data[self.datetime_column_name].min()
        return None

    @property
    def datetime_end(self):
        datetime_column_name = self.get_config().DataTMS.datetime_column_name
        if self.data is not None and datetime_column_name in self.data.columns:
            return self.data[self.datetime_column_name].max()
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

    @property
    def peak_max(self) -> Optional[Dict]:

        datetime_column_name = self.get_config().DataTMS.datetime_column_name
        main_value_column_name = self.get_config().DataTMS.main_value_column_name
        try:
            peak = find_max_peak(self.data, main_value_column_name, datetime_column_name)
        except Exception as e:
            logger.warning(f"No peak found for {self}, error: {e}")
            return None

        # For debugging
        show_peak: bool = False
        if show_peak:
            logger.info(f"Peak in {self}: {peak}")
        return peak

    @property
    def peaks(self):
        config = self.get_config().DataTMS
        datetime_column_name = config.datetime_column_name
        main_value_column_name = config.main_value_column_name
        n_peaks: int = config.n_peaks
        sample_rate: float = config.sample_rate
        min_time_diff: float = config.min_time_diff
        prominence: int = config.prominence

        try:
            peaks = find_n_peaks(self.data, main_value_column_name, datetime_column_name, n_peaks, sample_rate, min_time_diff, prominence)
        except Exception as e:
            logger.warning(f"No peaks found for {self.__str__()}, error: {e}")
            return None

        # For debugging
        show_peaks: bool = False
        if show_peaks:
            logger.info(f"Peaks found in {self.__str__()}: {peaks.__str__()}")
        return peaks

    def _validate_data(self) -> bool:
        """
        Checks if the DataFrame data is valid and contains the required columns.
        If data is not provided, it checks the instance's tms_df attribute.

        Args:
            data: DataFrame to validate. If not provided, self._tms_df is validated.

        Returns:
            bool: True if the DataFrame is valid, False otherwise.
        """
        try:
            validate_df(df=self.data, columns=self.get_config().DataTMS.tms_df_columns)
            return True
        except Exception as e:
            logger.error(f"Error during validation of the DataFrame: {e}")
            return False

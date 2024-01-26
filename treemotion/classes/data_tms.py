from kj_core.classes.core_data_class import CoreDataClass
from kj_core.df_utils.validate import validate_df

from ..common_imports.imports_classes import *
from .base_class_data_tms import BaseClassDataTMS

logger = get_logger(__name__)


class DataTMS(CoreDataClass, BaseClassDataTMS):
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
            tms_df = pd.read_csv(filepath, sep=";", parse_dates=["Time"], decimal=",", index_col='Time')
        except pd.errors.ParserError as e:
            logger.error(f"Error while reading the file {filepath.stem}. Please check the file format.")
            raise e
        except Exception as e:
            logger.error(f"Unusual error while loading {filepath.stem}: {e}")
            raise e
        return tms_df

    def validate_data(self) -> bool:
        """
        Checks if the DataFrame data is valid and contains the required columns.

        Returns:
            bool: True if the DataFrame is valid, False otherwise.
        """
        try:
            validate_df(df=self.data, columns=self.get_config().Data.data_tms_columns)
            logger.debug(f"Data validation for '{self}' correct!")
            return True
        except Exception as e:
            logger.error(f"Error during validation of the DataFrame: {e}")
            return False



    def time_cut(self, start_time: str, end_time: str, inplace: bool = False, auto_commit: bool = False) -> Union[pd.DataFrame, None]:
        """
        Limits the data to a specific time range and optionally updates the instance data in-place.

        Parameters
        ----------
        start_time : str
            The start time of the range, in a format compatible with `validate_time_format`.
        end_time : str
            The end time of the range, in a format compatible with `validate_time_format`.
        inplace : bool, optional
            If True, updates the instance's data in-place. Defaults to False.
        auto_commit : bool, optional
            If True, automatically commits changes to the database. Defaults to False.

        Returns
        -------
        Version
            Self-reference for method chaining.
        """
        from kj_core.df_utils.time_cut import validate_time_format, time_cut_by_datetime_index
        # Validate time formats
        validated_start_time = validate_time_format(start_time)
        validated_end_time = validate_time_format(end_time)

        if isinstance(validated_start_time, ValueError) or isinstance(validated_end_time, ValueError):
            logger.error("Invalid time format provided.")
            return

        # Attempt to limit data within the specified time range
        try:
            data = self.data.copy()
            data = time_cut_by_datetime_index(data,  start_time=start_time, end_time=end_time)

            if inplace:
                self.data = data

            if auto_commit:
                self.tempdrift_method = "haalllo"
                self.get_database_manager().commit()

            logger.info(f"Successfully limited the data of '{self}' between '{start_time}' and '{end_time}', inplace: '{inplace}', auto_commit: '{auto_commit}'.")

            return data
        except Exception as e:
            logger.error(f"Error limiting the data of '{self}': {e}")
            return

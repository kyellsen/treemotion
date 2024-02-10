from kj_core.classes.core_data_class import CoreDataClass
from kj_core.df_utils.validate import validate_df

from ..common_imports.imports_classes import *
from .base_class_data_tms import BaseClassDataTMS

logger = get_logger(__name__)


class DataTMS(CoreDataClass, BaseClassDataTMS):
    __tablename__ = 'DataTMS'
    data_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    data_filepath = Column(String, unique=True)
    data_changed = Column(Boolean)
    measurement_version_id = Column(Integer,
                                    ForeignKey('MeasurementVersion.measurement_version_id', onupdate='CASCADE'),
                                    nullable=False)
    tempdrift_method = Column(String)
    filter_method = Column(String)
    rotation_method = Column(String)

    def __init__(self, data_id: int = None, data: pd.DataFrame = None, data_filepath: str = None, data_changed: bool = False, datetime_added=None,
                 datetime_last_edit=None, measurement_version_id: int = None, tempdrift_method: str = None, filter_method: str = None, rotation_method: str = None):
        CoreDataClass.__init__(self, data_id=data_id, data=data, data_filepath=data_filepath, data_changed=data_changed,
                               datetime_added=datetime_added, datetime_last_edit=datetime_last_edit)
        BaseClassDataTMS.__init__(self, data=data, measurement_version_id=measurement_version_id,
                                  tempdrift_method=tempdrift_method, filter_method=filter_method,
                                  rotation_method=rotation_method)

    @classmethod
    def create_from_csv(cls, csv_filepath: str, data_filepath: str, measurement_version_id: int) -> Optional['DataTMS']:

        data: pd.DataFrame = cls.read_data_csv(csv_filepath)
        obj = cls(data=data, data_filepath=data_filepath, measurement_version_id=measurement_version_id)
        logger.info(f"Created new '{obj}'")
        return obj

    def update_from_csv(self, csv_filepath: str) -> Optional['DataTMS']:
        self.data = self.read_data_csv(csv_filepath)
        logger.info(f"Updated new '{self}'")

        return self

    @classmethod
    @dec_runtime
    def read_data_csv(cls, filepath: str) -> Optional[pd.DataFrame]:
        """
        Reads data from a CSV file.

        :param filepath: Path to the CSV file.
        :return: DataFrame with the read data.
        """
        filepath = Path(filepath)
        if not filepath.exists():
            logger.error(f"search_path {filepath} does not exist.")
            return

        config = cls.get_config().Data
        time_column: str = config.data_tms_time_column
        columns_and_dtypes: Dict = config.data_tms_columns_and_dtypes

        try:
            df = pd.read_csv(filepath, sep=";", parse_dates=[time_column], decimal=",", index_col=time_column, dtype=columns_and_dtypes)
        except pd.errors.ParserError as e:
            logger.error(f"Error while reading the file {filepath.stem}. Please check the file format.")
            raise e
        except Exception as e:
            logger.error(f"Unusual error while loading {filepath.stem}: {e}")
            raise e
        return df

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



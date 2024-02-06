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

    def __init__(self, data_id: int = None, data: pd.DataFrame = None, data_filepath: str = None, data_changed: bool = False, datetime_added=None,
                 datetime_last_edit=None, measurement_version_id: int = None, tempdrift_method: str = None, filter_method: str = None):
        CoreDataClass.__init__(self, data_id=data_id, data=data, data_filepath=data_filepath, data_changed=data_changed,
                               datetime_added=datetime_added, datetime_last_edit=datetime_last_edit)
        BaseClassDataTMS.__init__(self, data=data, measurement_version_id=measurement_version_id, tempdrift_method=tempdrift_method, filter_method=filter_method)

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

        type = 'float64'
        dtype = {
            'East-West-Inclination': type,
            'North-South-Inclination': type,
            'Absolute-Inclination': type,
            'Inclination direction of the tree': type,
            'Temperature': type,
            'East-West-Inclination - drift compensated': type,
            'North-South-Inclination - drift compensated': type,
            'Absolute-Inclination - drift compensated': type,
            'Inclination direction of the tree - drift compensated': type
        }

        try:
            tms_df = pd.read_csv(filepath, sep=";", parse_dates=["Time"], decimal=",", index_col='Time', dtype=dtype)
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



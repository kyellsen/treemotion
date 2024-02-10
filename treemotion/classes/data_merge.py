from kj_core.classes.core_data_class import CoreDataClass
from kj_core.df_utils.validate import validate_df

from ..common_imports.imports_classes import *
from .base_class_data_tms import BaseClassDataTMS

logger = get_logger(__name__)


class DataMerge(CoreDataClass, BaseClassDataTMS):
    __tablename__ = 'DataMerge'
    data_id = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    data_filepath = Column(String, unique=True)
    data_changed = Column(Boolean)
    datetime_added = Column(DateTime)
    datetime_last_edit = Column(DateTime)
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
        BaseClassDataTMS.__init__(self, data=data, measurement_version_id=measurement_version_id, tempdrift_method=tempdrift_method,
                                  filter_method=filter_method, rotation_method=rotation_method)

    def __str__(self):
        """
        Returns a string representation of the DataMerge instance.
        """
        return f"{self.__class__.__name__}: data_id='{self.data_id}', measurement_version={self.measurement_version_id}'"

    @classmethod
    def create_from_measurement_version(cls, measurement_version_id, data) -> 'DataMerge':
        try:
            # create data_filepath
            data_directory: Path = cls.get_config().data_directory
            folder: Path = cls.get_config().Data.data_merge_directory
            filename: str = cls.get_data_manager().get_new_filename(
                data_id=measurement_version_id,
                prefix=f"data_merge",
                file_extension="feather"
            )
            data_filepath = str(data_directory / folder / filename)

            obj = cls(data=data, data_filepath=data_filepath,
                      measurement_version_id=measurement_version_id)
            logger.info(f"New {obj}")
            session = cls.get_database_manager().session
            session.add(obj)

            return obj
        except Exception as e:
            logger.error(
                f"Error creating {cls.__name__} from from measurement_version_id: '{measurement_version_id}': {e}")
            raise

    def update_from_measurement_version(self, data) -> None:
        """
        Updates the object with data from the station.

        Raises:
            ValueError: Raised if data_wind_station is not set.
            Exception: Propagates any exceptions that occur during the update.
        """
        try:
            self.data = data

        except Exception as e:
            logger.error(f"Error in updating from station: {e}")
            raise

    def validate_data(self) -> bool:
        """
        Checks if the DataFrame data is valid and contains the required columns.

        Returns:
            bool: True if the DataFrame is valid, False otherwise.
        """
        try:
            validate_df(df=self.data, columns=self.get_config().Data.data_merge_columns)
            logger.debug(f"Data validation for '{self}' correct!")
            return True
        except Exception as e:
            logger.error(f"Error during validation of the DataFrame: {e}")
            return False

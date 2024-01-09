from ..common_imports.imports_classes import *

from .data_wind_station import DataWindStation
from .data_tms import DataTMS
from .data_merge import DataMerge
from .data_ls3 import DataLS3

logger = get_logger(__name__)


class MeasurementVersion(BaseClass):
    __tablename__ = 'MeasurementVersion'
    measurement_version_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    measurement_version_name = Column(String)
    measurement_id = Column(Integer, ForeignKey('Measurement.measurement_id', onupdate='CASCADE'), nullable=False)

    data_tms = relationship("DataTMS", backref="measurement_version", uselist=False, cascade='all, delete-orphan')
    data_ls3 = relationship("DataLS3", backref="measurement_version", uselist=False, cascade='all, delete-orphan')
    data_merge = relationship("DataMerge", backref="measurement_version", uselist=False, cascade='all, delete-orphan')

    def __init__(self, measurement_version_id=None, measurement_version_name=None, measurement_id=None,
                 data_tms_id=None, data_ls3_id=None, data_merge_id=None):
        super().__init__()
        self.measurement_version_id = measurement_version_id
        self.measurement_version_name = measurement_version_name
        self.measurement_id = measurement_id
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

        folder = cls.get_config().DataTMS.data_directory
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

    def add_data_merge(self, update_existing: bool = True) -> Optional[DataMerge]:
        logger.info(f"Processing add_data_merge for '{self}'")
        session = self.get_database_manager().session

        try:
            # Check for an existing DataWindStation with the given station_id
            existing_data: DataMerge = session.query(DataMerge).filter(
                DataMerge.measurement_version_id == self.measurement_version_id).first()

            if existing_data and not update_existing:
                data_merge = existing_data
                session.flush()
                logger.debug(f"Return existing {data_merge.__class__.__name__}, update_existing = '{update_existing}': '{data_merge}'")

            else:
                if not existing_data:   # Create a new instance
                    if self.measurement.series.data_wind_station:
                        data_wind_station: DataWindStation = self.measurement.series.data_wind_station
                        logger.debug(f"Found {DataWindStation.__class__.__name__}: '{data_wind_station}'")
                    else:
                        raise ValueError(f"Found no {DataWindStation.__class__.__name__} in: '{self}'")

                    if self.data_tms:
                        data_tms: DataTMS = self.data_tms
                        logger.debug(f"Found {DataTMS.__class__.__name__}: '{data_tms}'")
                    else:
                        raise ValueError(f"Found no {DataTMS.__class__.__name__} in: '{self}'")

                    data_merge = DataMerge.create_from_measurement_version(
                        measurement_version=self,
                        data_wind_station=data_wind_station,
                        data_tms=data_tms)

                    session.flush()
                    logger.debug(f"Created new {DataMerge.__class__.__name__}: '{data_merge}'")

                else: # Update existing  instance
                    pass
                    # data_merge = existing_data
                    # data_merge.update_from_measurement_version(measurement_version=self)
                    # session.flush()
                    # logger.debug(f"Update existing {DataMerge.__class__.__name__}, update_existing = '{update_existing}': '{data_merge}'")

            self.data_merge = data_merge
            self.get_database_manager().commit()

            return data_merge

        except Exception as e:
            logger.error(f"Error in add_data_merge: {e}")
            raise  # Optionally re-raise the exception to notify calling functions




from ..common_imports.imports_classes import *

# from .wind_measurement import WindMeasurement
from .data_tms import DataTMS
from .data_ls3 import DataLS3
from .data_wind import DataWind

logger = get_logger(__name__)


class MeasurementVersion(BaseClass):
    __tablename__ = 'MeasurementVersion'
    measurement_version_id = Column(Integer, primary_key=True, autoincrement=True, unique=True)
    measurement_version_name = Column(String)
    measurement_id = Column(Integer, ForeignKey('Measurement.measurement_id', onupdate='CASCADE'), nullable=False)

    data_tms = relationship("DataTMS", backref="measurement_version", uselist=False, cascade='all, delete-orphan')
    data_ls3 = relationship("DataLS3", backref="measurement_version", uselist=False, cascade='all, delete-orphan')
    data_wind = relationship("DataWind", backref="measurement_version", uselist=False, cascade='all, delete-orphan')

    def __init__(self, measurement_version_id=None, measurement_version_name=None, measurement_id=None,
                 data_tms_id=None, data_ls3_id=None, data_wind_id=None):
        super().__init__()
        self.measurement_version_id = measurement_version_id
        self.measurement_version_name = measurement_version_name
        self.measurement_id = measurement_id
        self.data_tms_id = data_tms_id
        self.data_ls3_id = data_ls3_id
        self.data_wind_id = data_wind_id

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

        m_v = cls(measurement_id=measurement_id, measurement_version_name=measurement_version_name)

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

        m_v.data_tms = data_tms

        # folder = cls.get_config().DataWind.data_directory
        # filename = (cls.
        #             get_data_manager().
        #             get_new_filename(measurement_id,
        #                              prefix=f"tms_{measurement_version_name}",
        #                              file_extension="feather"))
        # data_wind_filepath = data_directory / folder / filename
        #
        # data_wind = DataWind(data_id=None, data=DataWind.read_csv_tms(filepath=filepath_tms),
        #                      data_filepath=data_wind_filepath,
        #                      measurement_version_id=measurement_version_id)
        #
        # m_v.data_wind = data_wind

        session = m_v.get_database_manager().session
        session.add(m_v)
        return m_v

    def add_wind_from_station(self) -> Optional['MeasurementVersion']:
        """
        Loads TMS Data from a CSV file.
        """
        # get correct DataWindStation instance from Series
        data_wind_station = self.measurement.series.data_wind_station
        logger.debug(f"Found '{data_wind_station}'")

        data_directory = self.get_config().data_directory
        folder = self.get_config().DataWind.data_directory
        filename = (self.get_data_manager().
                    get_new_filename(data_id=self.measurement_version_id,
                                     prefix=f"wind_{self.measurement_version_name}",
                                     file_extension="feather"))
        data_wind_filepath = data_directory / folder / filename

        datetime_start = self.data_tms.datetime_start
        datetime_end = self.data_tms.datetime_end

        data =

        data_wind = DataWind(data=data,
                             data_filepath=data_wind_filepath,
                             measurement_version_id=self.measurement_version_id,
                             data_wind_station_id=data_wind_station.data_id)

        self.data_wind = data_wind
        self.get_database_manager().commit()
        return self

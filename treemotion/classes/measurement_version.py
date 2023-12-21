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
    # data_tms_id = Column(Integer)
    # data_ls3_id = Column(Integer)
    # data_wind_id = Column(Integer)
    datetime_start = Column(DateTime)
    datetime_end = Column(DateTime)
    duration = Column(Float)
    length = Column(Integer)
    tempdrift_method = Column(String)
    peak_index = Column(Integer)
    peak_time = Column(DateTime)
    peak_value = Column(Float)

    data_tms = relationship("DataTMS", backref="measurement_version", uselist=False, cascade='all, delete-orphan')
    data_ls3 = relationship("DataLS3", backref="measurement_version", uselist=False, cascade='all, delete-orphan')
    data_wind = relationship("DataWind", backref="measurement_version", uselist=False, cascade='all, delete-orphan')

    def __init__(self, measurement_version_id=None, measurement_version_name=None, measurement_id=None,
                 data_tms_id=None, data_ls3_id=None, data_wind_id=None, datetime_start=None, datetime_end=None,
                 duration=None, length=None, tempdrift_method=None, peak_index=None,
                 peak_time=None, peak_value=None):
        super().__init__()
        self.measurement_version_id = measurement_version_id
        self.measurement_version_name = measurement_version_name
        self.measurement_id = measurement_id
        self.data_tms_id = data_tms_id
        self.data_ls3_id = data_ls3_id
        self.data_wind_id = data_wind_id
        self.datetime_start = datetime_start
        self.datetime_end = datetime_end
        self.duration = duration
        self.length = length
        self.tempdrift_method = tempdrift_method
        self.peak_index = peak_index
        self.peak_time = peak_time
        self.peak_value = peak_value

    def __str__(self):
        return f"{self.__class__.__name__}(id={self.measurement_version_id}, measurement_version_name={self.measurement_version_name})"

    def get_filename(self) -> str:
        filename = f"{self.__class__.__name__}"
        return filename

    @classmethod
    def load_from_csv(cls, filepath_tms: str, measurement_id: int, measurement_version_id: int = None,
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

        data_filepath_tms = (m_v.
                             get_data_manager().
                             get_new_filepath(measurement_id,
                                              folder_name="tms",
                                              prefix=f"tms_{measurement_version_name}",
                                              file_extension="feather"))

        data_tms = DataTMS(data_id=None, data=cls.read_csv_tms(filepath=filepath_tms),
                           data_filepath=data_filepath_tms,
                           measurement_version_id=measurement_version_id)
        m_v.data_tms = data_tms

        session = m_v.get_database_manager().session
        session.add(m_v)
        return m_v

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

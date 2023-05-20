# treemotion/classes/wind_messreihe.py
from utilities.imports_classes import *

from utilities.base import Base
from utilities.dwd_download import download_dwd_txt

from sqlalchemy import func

from .wind_messung import WindMessung

logger = get_logger(__name__)


class WindMessreihe(Base):
    __tablename__ = 'WindMessreihe'
    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    name = Column(String)
    quelle = Column(String)
    datetime_added = Column(DateTime)
    datetime_last_edit = Column(DateTime)  # metadata
    datetime_start = Column(DateTime)  # metadata
    datetime_end = Column(DateTime)  # metadata
    duration = Column(Float)  # metadata
    length = Column(Integer)  # metadata

    messungen = relationship('WindMessung', backref='wind_messreihe', lazy="joined",
                             cascade="all, delete, delete-orphan", order_by="WindMessung.id")

    def __init__(self, id: int = None, name: str = None, quelle: str = None):
        """
        Constructor for WindMessreihe class.
        """
        self.id = id
        self.name = name
        self.quelle = quelle
        self.datetime_added = func.datetime('now')
        self.datetime_last_edit = func.datetime('now')
        self.datetime_start = None
        self.datetime_end = None
        self.duration = None
        self.length = None

    def __str__(self):
        return f"WindMessreihe(id={self.id}, name={self.name}"

    def update_metadata(self):
        """
        Aktualisiert die Metadaten der WindMessreihe basierend auf den verknüpften WindMessung-Instanzen.

        :return: True, wenn die Metadaten erfolgreich aktualisiert wurden, False bei einem Fehler.
        """
        try:
            logger.info("Start updating metadata for WindMessreihe.")
            # Filtern der Messungen nach der richtigen WindMessreihe
            messungen = self.messungen
            self.datetime_start = min([m.datetime for m in messungen])
            logger.debug(f"Startzeitpunkt der Zeitreihe: {self.datetime_start}")
            self.datetime_end = max([m.datetime for m in messungen])
            logger.debug(f"Endzeitpunkt der Zeitreihe: {self.datetime_end}")
            self.duration = (self.datetime_end - self.datetime_start).total_seconds()
            logger.debug(f"Dauer der Zeitreihe: {self.duration} Sekunden")
            self.length = len(messungen)
            logger.debug(f"Anzahl der Zeilen in der Zeitreihe: {self.length}")

            self.datetime_last_edit = func.datetime('now')
            logger.info("Metadata for WindMessreihe successfully updated.")

        except Exception as e:
            logger.error(f"Fehler beim Aktualisieren der Metadaten der WindMessreihe: {str(e)}")
            return False
        return True

    @classmethod
    def load_from_dwd_online(cls, name, stations_id, folder_path,
                             link_1="https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/wind/recent/",
                             link_2="https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/extreme_wind/recent/",
                             overwrite=False, auto_commit=False, session=None):
        """
        Loads wind measurement data from online DWD (Deutscher Wetterdienst) resources.

        :param name: Name of
        :param stations_id: Identifier for the station to load data from.
        :param folder_path: The folder path to store downloaded files.
        :param link_1: Link to online resource for average wind data.
        :param link_2: Link to online resource for extreme wind data.
        :return: An instance of WindMessreihe class filled with wind measurement data.
        """
        logger.info("Start loading data from online DWD resources.")
        folder_path, filename_1, filename_2 = download_dwd_txt(stations_id, folder_path, link_1, link_2)

        obj = cls.load_from_dwd_txt_file(name, folder_path, filename_1, filename_2, overwrite, auto_commit, session)
        logger.info("Data successfully loaded from online DWD resources.")
        return obj

    @classmethod
    def load_from_dwd_txt_file(cls,
                               name: str,
                               folder_path: Union[str, Path],
                               filename_1: str,
                               filename_2: str,
                               overwrite: bool = False,
                               auto_commit: bool = False,
                               session=None):
        """
        Loads wind measurement data from local txt files downloaded from DWD.

        :param name: Name of the WindMessreihe.
        :param folder_path: The folder path where the txt files are located.
        :param filename_1: Name of the first txt file.
        :param filename_2: Name of the second txt file.
        :param overwrite: Flag to indicate if existing data should be overwritten. Defaults to False.
        :param auto_commit: Flag to indicate if session should auto-commit. Defaults to False.
        :param session: Database session. Defaults to None.
        :return: An instance of WindMessreihe class filled with wind measurement data or None if there's an error.
        """
        logger.info("Start loading DWD data from pre-downloaded files.")
        folder_path = Path(folder_path)

        if not cls.check_files_exist(folder_path, filename_1, filename_2):
            return None

        try:
            merged_df = cls.load_and_prepare_dataframes(folder_path, filename_1, filename_2)
        except Exception as e:
            logger.error("Error while loading and preparing dataframes: " + str(e))
            return None

        wind_messreihe = cls._create_wind_messreihe(name, merged_df)
        if wind_messreihe is None:
            return None

        return cls._handle_db_session(session, wind_messreihe, auto_commit, overwrite)

    @classmethod
    def _create_wind_messreihe(cls, name: str, merged_df: DataFrame):
        """
        Creates an instance of WindMessreihe class from provided data.

        :param name: Name of the WindMessreihe.
        :param merged_df: Merged DataFrame with data.
        :return: An instance of WindMessreihe class filled with wind measurement data or None if there's an error.
        """
        try:
            obj = cls()
            obj.name = name
            obj.quelle = f"https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/"
            obj.datetime_added = func.datetime('now')

            for idx, row in merged_df.iterrows():
                messung = WindMessung(
                    id=None,
                    id_wind_messreihe=obj.id,
                    station_id=row['STATIONS_ID'],
                    datetime=row['MESS_DATUM'],
                    quality_level_wind_avg=row['QN_file1'],
                    wind_speed_10min_avg=row['FF_10'],
                    wind_direction_10min_avg=row['DD_10'],
                    quality_level_wind_extremes=row['QN_file2'],
                    wind_speed_max_10min=row['FX_10'],
                    wind_speed_min_10min=row['FNX_10'],
                    wind_speed_max_10min_moving_avg=row['FMX_10'],
                    wind_direction_max_wind_speed=row['DX_10']
                )
                obj.messungen.append(messung)
            obj.update_metadata()
        except Exception as e:
            logger.error("Error while creating WindMessung objects: " + str(e))
            return None

        return obj

    @staticmethod
    def _handle_db_session(session, wind_messreihe, auto_commit: bool,
                           overwrite: bool):
        """
        Handles database session.

        :param session: Database session. If None, a new session will be created.
        :param wind_messreihe: WindMessreihe instance to handle in the session.
        :param auto_commit: Flag to indicate if session should auto-commit.
        :param overwrite: Flag to indicate if existing data should be overwritten.
        :return: The wind_messreihe if the operation was successful, None otherwise.
        """
        try:
            session = db_manager.get_session(session)

            existing_obj = session.query(WindMessreihe).filter_by(name=wind_messreihe.name).first()
            if existing_obj is not None:
                if overwrite:
                    logger.warning(f"Existing object will be overwritten (overwrite = True): ")
                    session.delete(existing_obj)
                else:
                    logger.warning(f"Object already exists, not overwritten (overwrite = False), obj: ")
                    return existing_obj

            session.add(wind_messreihe)

            if auto_commit:
                session.commit()
                logger.info(f"Loading data from csv and committing to database successful (auto_commit=True)!")
            else:
                logger.info(f"Loading data from csv for successful (auto_commit=False)!")

            return wind_messreihe
        except Exception as e:
            logger.error("Error while handling the database session: " + str(e))
            return None

    @staticmethod
    def check_files_exist(folder_path: Path, filename_1: str, filename_2: str) -> bool:
        """
        Checks if given files exist at the provided folder path.

        :param folder_path: The Path where the txt files are located.
        :param filename_1: Name of the first txt file.
        :param filename_2: Name of the second txt file.
        :return: True if both files exist, False otherwise.
        """
        # Check if the files exist
        if not folder_path.joinpath(filename_1).exists() or not folder_path.joinpath(filename_2).exists():
            logger.error("One or both files do not exist.")
            return False
        return True

    @staticmethod
    def load_and_prepare_dataframes(folder_path: Path, filename_1: str, filename_2: str):
        """
        Loads and prepares the dataframes from the provided txt files.

        :param folder_path: The Path where the txt files are located.
        :param filename_1: Name of the first txt file.
        :param filename_2: Name of the second txt file.
        :return: Merged DataFrame with prepared data.
        """
        # Read each file into a DataFrame
        df1 = pd.read_csv(folder_path.joinpath(filename_1), sep=';', index_col=False)
        df2 = pd.read_csv(folder_path.joinpath(filename_2), sep=';', index_col=False)

        # Remove leading and trailing spaces from column names
        df1.columns = df1.columns.str.strip()
        df2.columns = df2.columns.str.strip()

        # Ensure 'MESS_DATUM' is in the correct date format
        df1['MESS_DATUM'] = pd.to_datetime(df1['MESS_DATUM'], format='%Y%m%d%H%M')
        df2['MESS_DATUM'] = pd.to_datetime(df2['MESS_DATUM'], format='%Y%m%d%H%M')

        # Merge the two DataFrames based on the 'STATIONS_ID' and 'MESS_DATUM' columns
        merged_df = pd.merge(df1, df2, on=['STATIONS_ID', 'MESS_DATUM'], suffixes=('_file1', '_file2'))

        # Remove the 'eor' columns
        merged_df = merged_df.drop(['eor_file1', 'eor_file2'], axis=1)

        # Replace -999 with None as -999 is marked as missing value
        merged_df = merged_df.replace(-999, None)

        return merged_df

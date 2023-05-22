# treemotion/classes/wind_messreihe.py
from utils.imports_classes import *

from utils.base import Base
from wind.wind_dwd_download import download_dwd_files
from utils.path_utils import validate_files_exist

from sqlalchemy import func

from .wind_messung import WindMessung

logger = get_logger(__name__)


class WindMessreihe(Base):
    __tablename__ = 'WindMessreihe'
    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    name = Column(String)
    station_id = Column(Integer)
    station_name = Column(String)
    bundesland = Column(String)
    station_hoehe = Column(Integer)
    station_geo_breite = Column(Float)
    station_geo_laenge = Column(Float)
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
        station_id = None
        station_name = None
        bundesland = None
        station_hoehe = None
        station_geo_breite = None
        station_geo_laenge = None
        self.quelle = quelle
        self.datetime_added = func.datetime('now')
        self.datetime_last_edit = func.datetime('now')
        self.datetime_start = None
        self.datetime_end = None
        self.duration = None
        self.length = None

    def __str__(self):
        return f"WindMessreihe(id={self.id}, name={self.name})"

    def get_wind_df(self, datetime_start=None, datetime_end=None, time_col: str = 'datetime', columns=None,
                      session=None):
        """
        Extracts wind data for the current instance from the Windmessung table as Pandas DataFrame.
        It filters the data by a specified time period and includes only specified columns if specified.

        Parameters
        ----------
        datetime_start : datetime-like, optional
            The start of the time period to extract data for.
            If None, extraction is not bounded by start time.
        datetime_end : datetime-like, optional
            The end of the time period to extract data for.
            If None, extraction is not bounded by end time.
        time_col : str, optional
            The name of the column to use for time filtering. Default is 'datetime'.
        columns : list of str, optional
            The columns to include in the output. If None, all columns are included.
        session : sqlalchemy.orm.Session, optional
            The session to use for the query. If None, a new session is created.

        Returns
        -------
        pandas.DataFrame
            A DataFrame containing the extracted data.

        Raises
        ------
        AttributeError
            If 'time_col' or any column in 'columns' do not exist in WindMessung.
        """
        try:
            session = db_manager.get_session(session)

            # Verify that the time_col exists
            if not hasattr(WindMessung, time_col):
                logger.error(f"Invalid column name for time_col: {time_col}")
                raise AttributeError(f"Invalid column name for time_col: {time_col}")

            if columns is not None:
                # Verify that all specified columns exist
                for col in columns:
                    if not hasattr(WindMessung, col):
                        logger.error(f"Invalid column name in columns: {col}")
                        raise AttributeError(f"Invalid column name in columns: {col}")
                query = session.query(*[getattr(WindMessung, col) for col in columns]).filter(
                    WindMessung.id_wind_messreihe == self.id)
            else:
                query = session.query(WindMessung).filter(WindMessung.id_wind_messreihe == self.id)

            if datetime_start is not None:
                query = query.filter(getattr(WindMessung, time_col) >= datetime_start)
            if datetime_end is not None:
                query = query.filter(getattr(WindMessung, time_col) <= datetime_end)

            logger.info(f"Executing query for WindMessreihe with id {self.id} from {datetime_start} to {datetime_end}")
            return pd.read_sql(query.statement, session.bind)
        except Exception as e:
            logger.error(f"Error while getting wind data for WindMessreihe with id {self.id}: {str(e)}")
            raise e

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
    def load_from_dwd_online(cls, name, stations_id, folder_path: str = None,
                             link_wind="https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/wind/recent/",
                             link_wind_extrem="https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/extreme_wind/recent/",
                             link_stations_liste="https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/10_minutes/wind/recent/zehn_min_ff_Beschreibung_Stationen.txt",
                             overwrite=False, auto_commit=False, session=None):
        """
        Loads wind measurement data from online DWD (Deutscher Wetterdienst) resources.

        :param name: Name of
        :param stations_id: Identifier for the station to load data from.
        :param folder_path: The folder path to store downloaded files.
        :param link_wind: Link to online resource for average wind data.
        :param link_wind_extrem: Link to online resource for extreme wind data.
        :return: An instance of WindMessreihe class filled with wind measurement data.
        """
        logger.info("Start loading data from online DWD resources.")

        if folder_path is None:
            folder_path = Path(configuration.working_directory) / configuration.dwd_data_directory

        folder_path, filename_wind, filename_wind_extreme, filename_stations_liste = download_dwd_files(stations_id,
                                                                                                        folder_path,
                                                                                                        link_wind,
                                                                                                        link_wind_extrem,
                                                                                                        link_stations_liste)

        obj = cls.load_from_dwd_txt_file(name, stations_id, folder_path, filename_wind, filename_wind_extreme,
                                         filename_stations_liste, overwrite, auto_commit, session)
        logger.info("Data successfully loaded from online DWD resources.")
        return obj

    @classmethod
    def load_from_dwd_txt_file(cls,
                               name: str,
                               stations_id: int,
                               folder_path: Union[str, Path],
                               filename_wind: str,
                               filename_wind_extreme: str,
                               filename_stations_liste: str,
                               overwrite: bool = False,
                               auto_commit: bool = False,
                               session=None):
        """
        Load wind measurement data from local txt files downloaded from the German Weather Service (DWD).

        :param name: Name of the WindMessreihe.
        :param stations_id: Identifier of the station from which the data originates.
        :param folder_path: Path to the folder where the txt files are located.
        :param filename_wind: Name of the txt file containing the wind data.
        :param filename_wind_extreme: Name of the txt file containing the extreme wind data.
        :param filename_stations_liste: Name of the txt file containing the list of stations.
        :param overwrite: If True, existing data will be overwritten. Defaults to False.
        :param auto_commit: If True, the database session will commit automatically. Defaults to False.
        :param session: Database session, if one is already open. Defaults to None.
        :return: An instance of WindMessreihe class filled with wind measurement data, or None if an error occurred.
        """
        logger.info("Start loading DWD data from pre-downloaded files.")
        folder_path = Path(folder_path)

        files = [filename_wind, filename_wind_extreme, filename_stations_liste]

        # Check if files exist
        for file in files:
            if not validate_files_exist(folder_path, file):
                return None

        try:
            merged_df = cls.load_and_prepare_dataframes(folder_path, filename_wind, filename_wind_extreme)
        except Exception as e:
            logger.error("Error while loading and preparing dataframes: " + str(e))
            return None

        # liest die stations_liste aus
        try:
            station_metadata = cls.load_and_prepare_station_metadata(stations_id, folder_path, filename_stations_liste)
        except Exception as e:
            logger.error(
                f"Error while loading and preparing stations_parameter from {filename_stations_liste}: " + str(e))
            return None

        wind_messreihe = cls._create_wind_messreihe(name, merged_df, station_metadata)
        if wind_messreihe is None:
            return None

        return cls._handle_db_session(session, wind_messreihe, auto_commit, overwrite)

    @staticmethod
    def load_and_prepare_dataframes(folder_path: Path, filename_wind: str, filename_wind_extreme: str):
        """
        Loads and prepares the dataframes from the provided txt files.

        :param folder_path: The Path where the txt files are located.
        :param filename_wind: Name of the first txt file.
        :param filename_wind_extreme: Name of the second txt file.
        :return: Merged DataFrame with prepared data.
        """
        # Read each file into a DataFrame
        df1 = pd.read_csv(folder_path.joinpath(filename_wind), sep=';', index_col=False)
        df2 = pd.read_csv(folder_path.joinpath(filename_wind_extreme), sep=';', index_col=False)

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
        logger.debug(f"Wind- und Wind-Extreme Daten aus {filename_wind} und {filename_wind_extreme} geladen!")
        return merged_df

    @staticmethod
    def load_and_prepare_station_metadata(stations_id: int, folder_path: str, filename_stations_liste: str) -> dict:
        """
        Load and prepare station data from a text file.

        :param stations_id: the id of the station
        :param folder_path: the folder where the file is located
        :param filename_stations_liste: the name of the file
        :return: a dictionary containing station metadata
        """
        # Dateinamen erstellen
        file_path = Path(folder_path) / filename_stations_liste

        # Durch die Zeilen der Datei iterieren
        with open(file_path, 'r', encoding='latin1') as f:
            next(f)  # überspringt die Kopfzeile
            next(f)  # überspringt die Zeile mit den Trennstrichen
            for line in f:
                line_data = line.split()
                if int(line_data[0]) == stations_id:
                    # Die gefundene Zeile in ein Dictionary konvertieren
                    station_metadata = {
                        "station_id": int(line_data[0]),
                        "von_datum": line_data[1],
                        "bis_datum": line_data[2],
                        "station_hoehe": int(line_data[3]),
                        "station_geo_breite": float(line_data[4]),
                        "station_geo_laenge": float(line_data[5]),
                        "station_name": " ".join(line_data[6:-1]),
                        "bundesland": line_data[-1]
                    }
                    logger.debug(f"Station_metadata aus {filename_stations_liste} geladen: {station_metadata.__str__()}")
                    return station_metadata

        raise ValueError(f"No data found for station id {stations_id}")

    @classmethod
    def _create_wind_messreihe(cls, name: str, merged_df: DataFrame, station_metadata: Dict):
        """
        Creates an instance of WindMessreihe class from provided data.

        :param name: Name of the WindMessreihe.
        :param merged_df: Merged DataFrame with data.
        :return: An instance of WindMessreihe class filled with wind measurement data or None if there's an error.
        """
        try:
            obj = cls()
            obj.name = name
            # aus metadata
            obj.station_id = station_metadata["station_id"]
            obj.station_name = station_metadata["station_name"]
            obj.bundesland = station_metadata["bundesland"]
            obj.station_hoehe = station_metadata["station_hoehe"]
            obj.station_geo_breite = station_metadata["station_geo_breite"]
            obj.station_geo_laenge = station_metadata["station_geo_laenge"]

            obj.quelle = f"https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/"
            obj.datetime_added = func.datetime('now')

            for idx, row in merged_df.iterrows():
                messung = WindMessung(
                    id=None,
                    id_wind_messreihe=obj.id,
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
        logger.debug(f"Objekt {obj.__str__()} erfolgreich erstellt!")
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

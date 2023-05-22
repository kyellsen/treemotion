# treemotion/classes/data.py
import pandas as pd
from sqlalchemy import text

from utils.imports_classes import *
from utils.path_utils import validate_and_get_filepath
from utils.dataframe_utils import validate_dataframe
from tms.time_utils import validate_time_format, limit_df_by_time, optimal_time_frame
from tms.find_peaks import find_max_peak, find_n_peaks

logger = get_logger(__name__)


class Data(BaseClass):
    __tablename__ = 'Data'
    id_data = Column(Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    id_messung = Column(Integer, ForeignKey('Messung.id_messung', onupdate='CASCADE'), nullable=False)
    version = Column(String)
    table_name = Column(String)
    datetime_start = Column(DateTime)
    datetime_end = Column(DateTime)
    duration = Column(Float)
    length = Column(Integer)
    tempdrift_method = Column(String)
    peak_index = Column(Integer)
    peak_time = Column(DateTime)
    peak_value = Column(Float)

    def __init__(self, *args, id_data: int = None, id_messung: int = None, version: str = None, table_name: str = None,
                 datetime_start: datetime = None, datetime_end: datetime = None, duration: datetime = None,
                 length: int = None, tempdrift_method: str = None, peak_index: int = None, peak_time: datetime = None,
                 peak_value: float = None, df: pd.DataFrame = None, df_wind: pd.DataFrame = None, **kwargs):
        """
        Initialisiert eine Data-Instanz.

        :param id_data: Eindeutige ID der Daten.
        :param id_messung: ID der Messung, zu der die Daten gehören.
        :param version: Version der Daten.
        :param table_name: Name der SQLite-Tabelle, in der die Daten gespeichert sind.
        :param datetime_start: Startzeitpunkt der Daten.
        :param datetime_end: Endzeitpunkt der Daten.
        :param duration: Dauer der Daten.
        :param length: Länge der Daten.
        :param df: Datenrahmen mit den Daten.
        """
        super().__init__(*args, **kwargs)
        # in SQLite Database
        self.id_data = id_data
        self.id_messung = id_messung
        self.version = version
        self.table_name = table_name  # name of SQLite Table, where data is stored
        self.datetime_start = datetime_start  # metadata
        self.datetime_end = datetime_end  # metadata
        self.duration = duration  # metadata
        self.length = length  # metadata
        self.tempdrift_method = tempdrift_method  # metadata
        self.peak_index = peak_index  # metadata
        self.peak_time = peak_time  # metadata
        self.peak_value = peak_value  # metadata
        # additional only in class-object
        self.peaks_indexs = None
        self.peaks_times = None
        self.peaks_values = None
        # additional only in class-object, own table in database "auto_df_{version}_{id_messung}_messung
        self.df = df

    def __str__(self):
        return f"Data(id={self.id_data}, table_name={self.table_name})"

    # Geerbt von BaseClass
    @classmethod
    @timing_decorator
    def load_from_db(cls, id_messung: int = None, load_related_df: bool = False, session=None):
        """
        Lädt Daten aus der Datenbank.

        :param id_messung: ID der Messung, zu der die Daten gehören.
        :param load_related_df: Ob der zugehörige Datenrahmen geladen werden soll.
        :param session: SQL-Alchemie-Session zur Interaktion mit der Datenbank.
        :return: Liste von Datenobjekten.
        """
        objs = super().load_from_db(filter_by={'id_messung': id_messung} if id_messung else None, session=session)
        if not objs:
            logger.error(f"Keine Daten gefunden für id_messung={id_messung}")
        else:
            logger.info(f"{len(objs)} Data-Objekte wurden erfolgreich geladen.")
        if load_related_df:
            for obj in objs:
                obj.load_df()
        return objs

    @classmethod
    @timing_decorator
    def load_from_csv(cls, filepath: str, id_data, id_messung: int, version: str, table_name: str):
        """
        Lädt Daten aus einer CSV-Datei.

        :param filepath: Pfad zur CSV-Datei.
        :param id_data: Eindeutige ID der Daten.
        :param id_messung: ID der Messung, zu der die Daten gehören.
        :param version: Version der Daten.
        :param table_name: Name der SQLite-Tabelle, in der die Daten gespeichert sind.
        :return: Datenobjekt.
        """
        if filepath is None:
            logger.warning(f"Filepath = None, Prozess abgebrochen.")
            return None
        obj = cls()
        obj.id_data = id_data
        obj.id_messung = id_messung
        obj.version = version
        obj.table_name = table_name
        try:
            obj.df = obj.read_csv_tms(filepath)
        except Exception as e:
            logger.error(f"Fehler beim Lesen der CSV Datei, error: {e}")
        obj.update_metadata()
        return obj

    @classmethod
    def create_new_version(cls, source_obj, version_new: str):
        """
        Erstellt eine neue Version des Datenobjekts.

        :param source_obj: Das Quellobjekt, das kopiert werden soll.
        :param version_new: Die neue Version.
        :return: Neues Datenobjekt.
        """
        new_obj = cls()
        try:
            new_obj.id_data = None
            new_obj.id_messung = source_obj.id_messung
            new_obj.version = version_new
            new_obj.table_name = new_obj.get_table_name(new_obj.version, new_obj.id_messung)
            new_obj.df = source_obj.df.copy(deep=True)  # nicht copy von Data, sondern auf pd.df
            new_obj.update_metadata()
            return new_obj
        except Exception as e:
            logger.error(f"Fehler beim Erstellen der Kopie der Dateninstanz: {e}")
            return None

    @timing_decorator
    def load_df(self, session=None):
        """
        Lädt die Daten für diese Data-Instanz aus der Datenbank.

        :param session: SQL-Alchemie-Session zur Interaktion mit der Datenbank.
        :return: Das Datenobjekt mit geladenen Daten oder None, wenn ein Fehler aufgetreten ist.
        """
        session = db_manager.get_session(session)
        try:
            self.df = pd.read_sql_table(self.table_name, session.bind)
            logger.info(f"Data.df erfolgreich geladen: {self.__str__()}")
        except Exception as e:
            logger.error(f"Data.df konnte nicht geladen werden: {self.__str__()}, error: {e}")
            return None
        return self

    def load_df_if_missing(self, session=None):
        """
        Lädt die Daten für diese Data-Instanz, falls sie noch nicht geladen wurden.

        :param session: SQL-Alchemie-Session zur Interaktion mit der Datenbank.
        :return: True, wenn das Laden erfolgreich war, sonst False.
        """
        if not hasattr(self, 'df') or self.df is None:
            logger.warning(
                f"Für Instanz {self.__str__()} fehlt der DataFrame (Data.df). Es wird automatisch Data.load_df() ausgeführt.")
            try:
                self.load_df(session=session)
                return True
            except Exception as e:
                logger.critical(f"Data.load_df konnte für {self.__str__()} nicht ausgeführt werden, error: {e}")
                return False

    @staticmethod
    @timing_decorator
    def read_csv_tms(filepath: str):
        """
        Liest Daten aus einer CSV-Datei.

        :param filepath: Pfad zur CSV-Datei.
        :return: Datenrahmen mit den gelesenen Daten.
        """
        try:
            filepath = validate_and_get_filepath(filepath)
        except Exception as e:
            raise e
        try:
            df = pd.read_csv(filepath, sep=";", parse_dates=["Time"], decimal=",", index_col=False)

        except pd.errors.ParserError as e:
            logger.error(f"Fehler beim Lesen der Datei {filepath.stem}. Überprüfen Sie das Dateiformat.")
            raise e
        except Exception as e:
            logger.error(f"Ungewöhnlicher Fehler beim Laden der {filepath.stem}: {e}")
            raise e

        return df

    @staticmethod
    def get_table_name(version: str, id_messung: int) -> str:
        """
        Erzeugt einen neuen Tabellennamen.

        :param version: Version der Daten.
        :param id_messung: ID der Messung, zu der die Daten gehören.
        :return: Neuer Tabellenname.
        """
        return f"auto_df_{version}_{str(id_messung).zfill(3)}_messung"

    def validate_dataframe(self, wind_data=False):
        """
        Überprüft, ob das DataFrame Data.df gültig und die benötigten Spalten vorhanden sind.
        """
        if not hasattr(self, 'df'):
            logger.error("Das Objekt hat kein Attribut 'df'.")
            return False
        if wind_data:
            df_columns = configuration.df_columns
            wind_df_columns_selected = configuration.wind_df_columns_selected
            try:
                validate_dataframe(self.df, columns=df_columns + wind_df_columns_selected)
            except Exception as e:
                logger.error(f"Fehler bei der Validierung des DataFrame: {e}")
                return False

        else:
            try:
                validate_dataframe(self.df, columns=configuration.df_columns)
            except Exception as e:
                logger.error(f"Fehler bei der Validierung des DataFrame: {e}")
                return False
        return True

    def update_metadata(self, auto_commit: bool = False, session=None):
        """
        Aktualisiert die Metadaten des Datenobjekts (Data.df).

        Überprüft zunächst, ob das DataFrame gültig ist. Wenn das DataFrame ungültig ist,
        gibt die Methode False zurück. Wenn das DataFrame gültig ist, aktualisiert sie
        die Metadaten und gibt True zurück.

        :param auto_commit:
        :param session:

        Returns
        -------
        bool
            Gibt True zurück, wenn die Aktualisierung der Metadaten erfolgreich war, und
            False, wenn das DataFrame ungültig ist oder ein Fehler aufgetreten ist.
        """

        if not self.validate_dataframe():
            return False

        try:
            self.datetime_start = pd.to_datetime(self.df['Time'].min(), format='%Y-%m-%d %H:%M:%S.%f')
            self.datetime_end = pd.to_datetime(self.df['Time'].max(), format='%Y-%m-%d %H:%M:%S.%f')
            self.duration = (self.datetime_end - self.datetime_start).total_seconds()
            self.length = len(self.df)
            peak = self.find_max_peak()
            if peak is None:
                logger.warning(f"Kein Peak für {self.__str__()}, sonstige Metadaten für {self.__str__()} aktualisiert!")
                if auto_commit:
                    self.commit(df_commit=False, session=session)
                return True
            self.peak_index = peak['peak_index']
            self.peak_time = peak['peak_time']
            self.peak_value = peak['peak_value']
            logger.info(f"Metadaten für {self.__str__()} erfolgreich aktualisiert!")
            if auto_commit:
                self.commit(df_commit=False, session=session)
        except (KeyError, ValueError) as e:
            logger.error(f"Metadaten für {self.__str__()} konnten nicht aktualisiert werden: {e}")
            return False
        return True

    def find_max_peak(self, show_peak: bool = False, value_col: str = "Absolute-Inclination - drift compensated",
                      time_col: str = "Time"):
        result = self.validate_dataframe()
        if not result:
            return None
        try:
            peak = find_max_peak(self.df, value_col, time_col)
        except Exception as e:
            logger.warning(f"Kein Peak für {self.__str__()} gefunden, error: {e}")
            return None

        if show_peak:
            logger.info(f"Peak in {self.__str__()}: {peak.__str__()}")
        return peak

    def find_n_peaks(self, show_peaks: bool = False, values_col: str = 'Absolute-Inclination - drift compensated',
                     time_col: str = 'Time', n_peaks: int = 10, sample_rate: float = 20,
                     min_time_diff: float = 60, prominence: int = None):
        result = self.validate_dataframe()
        if not result:
            return None
        try:
            peaks = find_n_peaks(self.df, values_col, time_col, n_peaks, sample_rate, min_time_diff, prominence)
        except Exception as e:
            logger.warning(f"Keine Peaks für {self.__str__()} gefunden, error: {e}")
            return None
        if show_peaks:
            logger.info(f"Peaks in {self.__str__()} gefunden: {peaks.__str__()}")
        return peaks

    # Geerbt von BaseClass
    @timing_decorator
    def copy(self, reset_id: bool = False, auto_commit: bool = False, session=None):
        """
        Erstellt eine Kopie des Datenobjekts.

        :param reset_id: Ob die ID des neuen Objekts zurückgesetzt werden soll.
        :param auto_commit: Ob ein automatischer Commit nach dem Kopieren erfolgen soll.
        :param session: SQL-Alchemie-Session zur Interaktion mit der Datenbank.
        :return: Kopiertes Datenobjekt.
        """
        new_obj = super().copy('id_data', reset_id, auto_commit, session)

        # Create a deep copy of the DataFrame
        if self.df is not None:
            new_obj.df = self.df.copy(deep=True)

        return new_obj

    @timing_decorator
    def commit(self, df_commit=True, session=None):
        """
        Fügt das Datenobjekt zur Datenbank hinzu und führt den Commit aus.

        :param df_commit: Wenn True wird Data.df commited, wenn False wird Data.df nicht commited (default).
        :param session: SQL-Alchemie-Session zur Interaktion mit der Datenbank.
        :return: True, wenn commit erfolgreich; False, wenn fehlgeschlagen
        """
        session = db_manager.get_session(session)
        try:
            session.add(self)
            if self.df is not None and df_commit:
                self.df.to_sql(self.table_name, session.bind, if_exists='replace', index=False)
            db_manager.commit(session)
            logger.info(f"Instanz '{self.__str__()}' zur Datenbank committed.")
            return True
        except Exception as e:
            session.rollback()
            logger.error(f"Fehler beim Commiten '{self.__str__()}' zur Datenbank: {e}")
            return False

    # Geerbt von BaseClass
    def remove(self, session=None, **kwargs):
        """
        Entfernt das Datenobjekt aus der Datenbank.

        :param session: SQL-Alchemie-Session zur Interaktion mit der Datenbank.
        """
        session = db_manager.get_session(session)
        existing_obj = session.query(type(self)).get(getattr(self, 'id_data'))
        try:
            if existing_obj is not None:
                # Delete the table associated with this Data object
                drop_table_statement = text(f"DROP TABLE IF EXISTS {self.table_name}")
                session.execute(drop_table_statement)
                self.df = None
                session.delete(existing_obj)
                logger.info(f"Objekt {self.__class__.__name__} wurde entfernt.")
                db_manager.commit(session)
                return True
            else:
                logger.info(f"Objekt {self.__class__.__name__} ist nicht vorhanden.")
                return False
        except Exception as e:
            session.rollback()  # Rollback the changes on error
            logger.error(f"Fehler beim Entfernen des Objekts {self.__class__.__name__}: {e}")
            return False

    # Geerbt von BaseClass
    def limit_by_time(self, start_time: str, end_time: str, auto_commit: bool = False, session=None):
        """
        Begrenzt die Daten auf einen bestimmten Zeitraum.

        :param start_time: Startzeitpunkt der Begrenzung.
        :param end_time: Endzeitpunkt der Begrenzung.
        :param auto_commit: Ob ein automatischer Commit nach dem Begrenzen des Zeitraums erfolgen soll.
        :param session: SQL-Alchemie-Session zur Interaktion mit der Datenbank.
        :return: True wenn erfolgreich, False wenn Fehler
        """
        # Überprüfung der Zeitangaben
        start_time = validate_time_format(start_time)
        end_time = validate_time_format(end_time)
        if start_time is None or end_time is None:
            logger.error(f"Das Zeitformat ist ungültig.")
            return False

        # Überprüfung des DataFrames
        if self.df is None or self.df.empty:
            logger.warning(f"Der DataFrame von {self.__str__()} ist None oder leer.")
            return False

        # Limitierung Zeit
        try:
            self.df = limit_df_by_time(self.df, time_col="Time", start_time=start_time, end_time=end_time)
        except Exception as e:
            logger.error(f"Fehler der Limitierung der Daten von '{self.__str__()}', error: {str(e)}")
            return False

        logger.debug(f"Limitierung der Daten von '{self.__str__()}' zwischen {start_time} und {end_time} erfolgreich.")
        self.update_metadata()

        if auto_commit:
            self.commit(session=session)

        return True

    def limit_time_by_peaks(self, duration: int, values_col: str = 'Absolute-Inclination - drift compensated',
                            time_col: str = 'Time', n_peaks: int = 10,
                            sample_rate: float = 20, min_time_diff: float = 60,
                            prominence: int = None, auto_commit: bool = False, session=None):

        # Überprüfung des DataFrames
        if self.df is None or self.df.empty:
            logger.warning(f"Der DataFrame von {self.__str__()} ist None oder leer.")
            return False

        # Überprüfen, ob die Spalten im DataFrame vorhanden sind
        if values_col not in self.df.columns or time_col not in self.df.columns:
            logger.warning(f"Die Spalten {values_col} und/oder {time_col} existieren nicht im DataFrame.")
            return False

        peaks_dict = find_n_peaks(self.df, values_col, time_col, n_peaks, sample_rate, min_time_diff, prominence)

        timeframe_dict = optimal_time_frame(duration, peaks_dict)
        self.df = limit_df_by_time(self.df, time_col="Time", start_time=timeframe_dict['start_time'],
                                   end_time=timeframe_dict['end_time'])

        logger.info(
            f"Limitierung der Daten von '{self.__str__()}' zwischen {timeframe_dict['start_time']} und {timeframe_dict['end_time']} erfolgreich.")
        self.update_metadata()
        if auto_commit:
            self.commit(session=session)

        return True

    def random_sample(self, n: int, auto_commit: bool = False, session=None):
        """
        Wählt eine zufällige Stichprobe von Daten aus und behält die ursprüngliche Reihenfolge bei.

        :param n: Anzahl der auszuwählenden Datenpunkte.
        :param auto_commit: Ob ein automatischer Commit nach der Auswahl erfolgen soll.
        :param session: SQL-Alchemie-Session zur Interaktion mit der Datenbank.
        :return: Selbstreferenz für Methodenverkettung.
        """
        logger.info(f"Zufällige Stichprobe von {n} Datenpunkten wird aus {self.__str__()} ausgewählt.")

        if n > len(self.df):
            logger.warning(
                f"Die N ({n}) ist größer als die Länge des DataFrames ({len(self.df)}). Es werden alle Datenpunkte ausgewählt.")
            n = len(self.df)

        try:
            sampled_indices = self.df.sample(n).index
            sampled_indices = sorted(sampled_indices)
            self.df = self.df.loc[sampled_indices]
            self.update_metadata()
            logger.debug(f"Zufällige Stichprobe von {n} Datenpunkten wurde ausgewählt: {self.__str__()}")
        except Exception as e:
            logger.error(f"Fehler beim Auswählen der zufälligen Stichprobe: {e}")
            return self

        if auto_commit:
            self.commit(session=session)

        return self

    #
    # @timing_decorator
    # def temp_drift_comp(self, method="emd_hht", overwrite=True, sample_rate=20, window_size=1000,
    #                     freq_range=(0.05, 2, 128), feedback=False):  # 128 is used because ...
    #     """
    #     Compensate the temperature drift in the measurements using the specified method.
    #
    #     :param method: The method to use for temperature drift compensation.
    #     :param overwrite: Whether to overwrite the original data or create new columns.
    #     :param sample_rate: The sample rate of the data (in Hz).
    #     :param window_size: The window size for the moving average method.
    #     :param freq_range: The frequency range for the EMD-HHT method.
    #     :param feedback: Show result and runtime
    #     """
    #     temp = self.df['Temperature']
    #
    #     methods = {
    #         "lin_reg": lambda df: tempdrift.temp_drift_comp_lin_reg(df, temp),
    #         "lin_reg_2": lambda df: tempdrift.temp_drift_comp_lin_reg_2(df, temp),
    #         "mov_avg": lambda df: tempdrift.temp_drift_comp_mov_avg(df, window_size),
    #         "emd_hht": lambda df: tempdrift.temp_drift_comp_emd(df, sample_rate, freq_range),
    #     }
    #
    #     if method in methods:
    #         x = methods[method](self.df['East-West-Inclination'])
    #         y = methods[method](self.df['North-South-Inclination'])
    #     else:
    #         raise ValueError(f"Invalid method for temp_drift_comp: {method}")
    #
    #     suffix = "" if overwrite else " - new"
    #
    #     self.df[f'East-West-Inclination - drift compensated{suffix}'] = x
    #     self.df[f'North-South-Inclination - drift compensated{suffix}'] = y
    #     self.df[f'Absolute-Inclination - drift compensated{suffix}'] = tms_basics.get_absolute_inclination(x, y)
    #     self.df[
    #         f'Inclination direction of the tree - drift compensated{suffix}'] = tms_basics.get_inclination_direction(
    #         x, y)
    #     if feedback is True:
    #         print(
    #             f"Messung.temp_drift_comp - id_messung: {self.id_messung}")
    #
    # def plot_df(self, y_cols):
    #     """
    #     Plots the specified columns of df against time.
    #
    #     Args:
    #         y_cols (list of str): The names of the columns to plot on the y-axis.
    #
    #     Returns:
    #         None.
    #
    #     Raises:
    #         KeyError: If any of the specified column names are not in the df.
    #     """
    #     fig, ax = plt.subplots()
    #     x = self.df["Time"]
    #     for col in y_cols:
    #         try:
    #             y = self.df[col]
    #         except KeyError:
    #             raise KeyError(f"Column '{col}' not found in df.")
    #         ax.plot(x, y, label=col)
    #     ax.set_xlabel("Time")
    #     ax.legend()
    #     plt.show()
    #
    # def plot_df_sub(self, y_cols):
    #     """
    #     Plot multiple time series subplots.
    #
    #     Parameters:
    #     y_cols (list of str): The columns to plot.
    #
    #     Returns:
    #     None
    #
    #     Raises:
    #         KeyError: If any of the specified column names are not in the df.
    #     """
    #     num_plots = len(y_cols)
    #     fig, axs = plt.subplots(num_plots, 1, figsize=(8, num_plots * 4))
    #     x = self.df["Time"]
    #     for i, col in enumerate(y_cols):
    #         try:
    #             y = self.df[col]
    #         except KeyError:
    #             raise KeyError(f"Column '{col}' not found in df.")
    #         axs[i].plot(x, y, label=col)
    #         axs[i].set_xlabel("Time")
    #         axs[i].legend()
    #     plt.show()
